using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s.Fluent;
using k8s.Models;
using Newtonsoft.Json;
using SPDY;

namespace k8s
{
	/// <summary>Implements the Kubernetes exec protocol on top of a <see cref="SPDYConnection"/>.</summary>
	public sealed class SPDYExec : IDisposable
	{
		/// <summary>Initializes a new <see cref="SPDYExec"/> on top of a new, open <see cref="SPDYConnection"/>.</summary>
		/// <param name="conn">An open <see cref="SPDYConnection"/></param>
		/// <param name="headers">The HTTP headers received from the request to upgrade the connection to SPDY.</param>
		/// <param name="stdin">A stream containing data to send over standard input, or null if no data should be sent</param>
		/// <param name="stdout">A stream containing data to receive from standard output, or null if no data should be received</param>
		/// <param name="stderr">A stream containing data to receive from the standard error stream, or null if no data should be received</param>
		/// <remarks>The set of streams passed to this method must match the set of streams described in the HTTP request to Kubernetes.</remarks>
		public SPDYExec(SPDYConnection conn, System.Net.Http.Headers.HttpHeaders headers,
			Stream stdin = null, Stream stdout = null, Stream stderr = null)
		{
			if(conn == null) throw new ArgumentNullException(nameof(conn));
			if(headers == null) throw new ArgumentNullException(nameof(headers));
			string version = headers.GetValues("X-Stream-Protocol-Version").FirstOrDefault();
			if(version == "v4.channel.k8s.io") this.version = 4; // figure out which exec protocol version we're using
			else if(version == "v3.channel.k8s.io") this.version = 3;
			else if(version == "v2.channel.k8s.io") this.version = 2;
			else // we don't support v1 because it's flawed
			{
				conn.Dispose();
				throw new NotSupportedException("Unsupported or missing exec protocol: " + version);
			}

			client = new SPDYClient(conn) { UseFlowControl = false }; // Kubernetes doesn't use or respect flow control
			(userStdin, userStdout, userStderr) = (stdin, stdout, stderr);
		}

		/// <summary>Runs the command being executed remotely.</summary>
		/// <param name="cancelToken">A <see cref="CancellationToken"/> that can be used to abort the command</param>
		/// <returns>Returns a <see cref="V1Status"/> object describing the result of executing the command. The <see cref="V1Status.Code"/>
		/// property will contain the command's exit code if known, or -1 if unknown.
		/// </returns>
		public Task<V1Status> RunAsync(CancellationToken cancelToken = default) => RunAsync(Timeout.Infinite, cancelToken);

		/// <summary>Runs the command being executed remotely.</summary>
		/// <param name="timeoutMs">A timeout, in milliseconds, after which the command will be aborted, or <see cref="Timeout.Infinite"/>
		/// if the command should not time out.
		/// </param>
		/// <param name="cancelToken">A <see cref="CancellationToken"/> that can be used to abort the command</param>
		/// <returns>Returns a <see cref="V1Status"/> object describing the result of executing the command. The <see cref="V1Status.Code"/>
		/// property will contain the command's exit code if known, or -1 if unknown.
		/// </returns>
		public async Task<V1Status> RunAsync(int timeoutMs, CancellationToken cancelToken = default)
		{
			if(timeoutMs < -1) throw new ArgumentOutOfRangeException(nameof(timeoutMs), "Must be non-negative or Timeout.Infinite.");
			SPDYStream error = null, stdin = null, stdout = null, stderr = null;
			CancellationTokenSource cts = null;
			if(timeoutMs >= 0)
			{
				cts = cancelToken.CanBeCanceled ? CancellationTokenSource.CreateLinkedTokenSource(cancelToken) : new CancellationTokenSource();
				cts.CancelAfter(timeoutMs);
				cancelToken = cts.Token;
			}
			try
			{
				Task stdinCopy = null;
				client.ShouldAccept = s => false; // the client creates all streams. don't accept streams created by the server
				if(userStdin != null) // although the SPDY protocol allows it, Kubernetes doesn't like it when we send data before it has
				{                     // accepted our streams - not just the one we're sending on but all of them, so if we're going to send...
					int desiredStreams = 2 + (userStdout != null ? 1 : 0) + (userStderr != null ? 1 : 0), acceptedStreams = 0;
					client.StreamOpened += s => // add an event handler called when Kubernetes accepts one of our streams
					{
						if(++acceptedStreams == desiredStreams) // if all the streams have been accepted...
						{
							stdinCopy = CopyAsync(userStdin, stdin, cancelToken); // start writing data to the STDIN stream
							stdinCopy.ContinueWith(t => stdin.Dispose(), // when userStdin is closed, close stdin to communicate the closure
								TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnRanToCompletion);
						}
					};
				}

				Task runTask = client.RunAsync(cancelToken, cancelToken); // start the SPDY client
				// create all streams before copying anything because the server won't start running the program until all the streams
				// have been created, and if we clog up the server with data before it starts running the program, it may block
				error = CreateStream(true, false, "error"); // create the "error" stream, which contains the result
				if(userStdin != null) stdin = CreateStream(false, true, "stdin");
				if(userStdout != null) stdout = CreateStream(true, false, "stdout");
				if(userStderr != null) stderr = CreateStream(true, false, "stderr");
				// begin copying the output streams
				Task stdoutCopy = CopyAsync(stdout, userStdout, cancelToken);
				Task stderrCopy = CopyAsync(stderr, userStderr, cancelToken);
				// read the error stream into a buffer. when the error stream is closed, the command has completed
				var ms = new MemoryStream();
				var tasks = new List<Task>(3) { CopyAsync(error, ms, cancelToken) };
				if(stdoutCopy != null) tasks.Add(stdoutCopy);
				if(stderrCopy != null) tasks.Add(stderrCopy);
				await Task.WhenAll(tasks).ConfigureAwait(false); // wait for the output copies to complete
				client.GoAway(); // tell the server that its job has been made redundant
				V1Status status;
				if(ms.Length == 0) // if the command was successful, but we have no "error" output...
				{
					status = new V1Status() { Status = "Success", Code = 0 }; // create a generic success status
				}
				else // otherwise, the command failed or, on version 4+, may have succeeded with a V1Status output
				{
					ms.Position = 0;
					var sr = new StreamReader(ms);
					if(version >= 4) // if the server should have returned a V1Status object...
					{
						status = FluentExtensions.DefaultSerializer.Deserialize<V1Status>(new JsonTextReader(sr));
						status.Code = 0; // assume success
						if(status.Status == "Failure")
						{
							// try to extract the exit code from the status and store it in the Code field
							V1StatusCause cause = status.Details?.Causes?.FirstOrDefault(c => c.Reason == "ExitCode");
							status.Code = cause != null ? int.Parse(cause.Message, System.Globalization.CultureInfo.InvariantCulture) : -1;
						}
					}
					else // otherwise, the server just returned an error string
					{
						status = new V1Status() { Status = "Failure", Reason = "CommandFailed", Code = -1, Message = sr.ReadToEnd() };
					}
				}
				await runTask.ConfigureAwait(false); // wait for the SPDY client to shut down gracefully
				return status;
			}
			finally
			{
				error?.Dispose();
				stdin?.Dispose();
				stdout?.Dispose();
				stderr?.Dispose();
				client.Dispose();
				cts?.Dispose();
			}
		}

		/// <inheritdoc/>
		public void Dispose() => client.Dispose();

		SPDYStream CreateStream(bool canRead, bool canWrite, string type) =>
			client.CreateStream(canRead, canWrite,
				headers: new Dictionary<string, List<string>>(1) { { "streamtype", new List<string>(1) { type } } });

		readonly SPDYClient client;
		readonly Stream userStdin, userStdout, userStderr;
		readonly int version;

#if NETCOREAPP2_1
		static Task CopyAsync(Stream src, Stream dest, CancellationToken cancelToken) => src?.CopyToAsync(dest, cancelToken);
#else
		static Task CopyAsync(Stream src, Stream dest, CancellationToken cancelToken) =>
			src?.CopyToAsync(dest, 81920, cancelToken); // use the same buffer size that CopyToAsync(dest) does
#endif
	}
}
