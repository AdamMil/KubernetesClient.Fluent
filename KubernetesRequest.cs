using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using k8s.Models;
using Microsoft.Rest;
using Newtonsoft.Json;
using SPDY;

namespace k8s.Fluent
{
	/// <summary>Represents a single request to Kubernetes.</summary>
	public sealed class KubernetesRequest : ICloneable
	{
		/// <summary>Initializes a <see cref="KubernetesRequest"/> based on a <see cref="Kubernetes"/> client.</summary>
		/// <param name="client">A <see cref="Kubernetes"/> client</param>
		/// <param name="scheme">The <see cref="KubernetesScheme"/> used to map types to their Kubernetes group, version, and kind, or
		/// null to use the <see cref="KubernetesScheme.Default"/> scheme. The default is null.
		/// </param>
		public KubernetesRequest(Kubernetes client, KubernetesScheme scheme = null)
		{
			if(client == null) throw new ArgumentNullException(nameof(client));
			(baseUri, credentials, this.client) = (client.BaseUri.ToString(), client.Credentials, client.HttpClient);
			Scheme(scheme);
		}

		/// <summary>Initializes a <see cref="KubernetesRequest"/> based on a <see cref="KubernetesClientConfiguration"/> and
		/// an <see cref="HttpClient"/>.
		/// </summary>
		/// <param name="config">The <see cref="KubernetesClientConfiguration"/> used to connect to Kubernetes</param>
		/// <param name="client">The <see cref="HttpClient"/> used to make the request, or null to use the default client. The default
		/// is null.
		/// </param>
		/// <param name="scheme">The <see cref="KubernetesScheme"/> used to map types to their Kubernetes group, version, and kind, or
		/// null to use the <see cref="KubernetesScheme.Default"/> scheme. The default is null.
		/// </param>
		/// <remarks>Any necessary SSL configuration must have already been applied to the <paramref name="client"/>.</remarks>
		public KubernetesRequest(KubernetesClientConfiguration config, HttpClient client = null, KubernetesScheme scheme = null)
		{
			if(config == null) throw new ArgumentNullException(nameof(config));
			this.baseUri = config.Host;
			if(string.IsNullOrEmpty(this.baseUri)) throw new ArgumentException(nameof(config)+".Host");
			this.client = client ?? new HttpClient();
			Scheme(scheme);
			if(!string.IsNullOrEmpty(config.AccessToken))
			{
				credentials = new TokenCredentials(config.AccessToken);
			}
			else if(!string.IsNullOrEmpty(config.Username))
			{
				credentials = new BasicAuthenticationCredentials() { UserName = config.Username, Password = config.Password };
			}
		}

		/// <summary>Initializes a <see cref="KubernetesRequest"/> based on a <see cref="Uri"/> and an <see cref="HttpClient"/>.</summary>
		/// <param name="baseUri">The absolute base URI of the Kubernetes API server</param>
		/// <param name="credentials">The <see cref="ServiceClientCredentials"/> used to connect to Kubernetes, or null if no credentials
		/// of that type are needed. The default is null.
		/// </param>
		/// <param name="client">The <see cref="HttpClient"/> used to make the request, or null to use the default client. The default
		/// is null.
		/// </param>
		/// <param name="scheme">The <see cref="KubernetesScheme"/> used to map types to their Kubernetes group, version, and kind, or
		/// null to use the <see cref="KubernetesScheme.Default"/> scheme. The default is null.
		/// </param>
		/// <remarks>Any necessary SSL configuration must have already been applied to the <paramref name="client"/>.</remarks>
		public KubernetesRequest(Uri baseUri, ServiceClientCredentials credentials = null, HttpClient client = null, KubernetesScheme scheme = null)
		{
			if(baseUri == null) throw new ArgumentNullException(nameof(baseUri));
			if(!baseUri.IsAbsoluteUri) throw new ArgumentException("The base URI must be absolute.", nameof(baseUri));
			(this.baseUri, this.credentials, this.client) = (baseUri.ToString(), credentials, client ?? new HttpClient());
			Scheme(scheme);
		}

		/// <summary>Gets the value of the Accept header, or null to use the default of application/json.</summary>
		public string Accept() => _accept;

		/// <summary>Sets the value of the Accept header, or null or empty to use the default of application/json.</summary>
		public KubernetesRequest Accept(string mediaType) { _accept = NormalizeEmpty(mediaType); return this; }

		/// <summary>Adds a header to the request. Multiple header values with the same name can be set this way.</summary>
		public KubernetesRequest AddHeader(string key, string value) => Add(ref headers, CheckHeaderName(key), value);

		/// <summary>Adds a header to the request.</summary>
		public KubernetesRequest AddHeader(string key, IEnumerable<string> values) => Add(ref headers, CheckHeaderName(key), values);

		/// <summary>Adds a header to the request.</summary>
		public KubernetesRequest AddHeader(string key, params string[] values) => Add(ref headers, CheckHeaderName(key), values);

		/// <summary>Adds a query-string parameter to the request. Multiple parameters with the same name can be set this way.</summary>
		public KubernetesRequest AddQuery(string key, string value) => Add(ref query, key, value);

		/// <summary>Adds query-string parameters to the request.</summary>
		public KubernetesRequest AddQuery(string key, IEnumerable<string> values) => Add(ref query, key, values);

		/// <summary>Adds query-string parameters to the request.</summary>
		public KubernetesRequest AddQuery(string key, params string[] values) => Add(ref query, key, values);

		/// <summary>Gets the body to be sent to the server.</summary>
		public object Body() => _body;

		/// <summary>Sets the body to be sent to the server. If null, no body will be sent. If a string, byte array, or stream, the
		/// contents will be sent directly. Otherwise, the body will be serialized into JSON and sent.
		/// </summary>
		public KubernetesRequest Body(object body) { _body = body; return this; }

		/// <summary>Clears custom header values with the given name.</summary>
		public KubernetesRequest ClearHeader(string headerName)
		{
			if(headerName == null) throw new ArgumentNullException(nameof(headerName));
			CheckHeaderName(headerName);
			if(headers != null) headers.Remove(headerName);
			return this;
		}

		/// <summary>Clears all custom header values.</summary>
		public KubernetesRequest ClearHeaders()
		{
			if(headers != null) headers.Clear();
			return this;
		}

		/// <summary>Clears all query-string parameters.</summary>
		public KubernetesRequest ClearQuery()
		{
			if(query != null) query.Clear();
			return this;
		}

		/// <summary>Clears all query-string parameters with the given key.</summary>
		public KubernetesRequest ClearQuery(string key)
		{
			if(key == null) throw new ArgumentNullException(nameof(key));
			if(query != null) query.Remove(key);
			return this;
		}

		/// <summary>Creates a deep copy of the <see cref="KubernetesRequest"/>.</summary>
		public KubernetesRequest Clone()
		{
			var clone = (KubernetesRequest)MemberwiseClone();
			if(headers != null)
			{
				clone.headers = new Dictionary<string, List<string>>(headers.Count);
				foreach(KeyValuePair<string, List<string>> pair in headers) clone.headers.Add(pair.Key, new List<string>(pair.Value));
			}
			if(query != null)
			{
				clone.query = new Dictionary<string, List<string>>(query.Count);
				foreach(KeyValuePair<string, List<string>> pair in query) clone.query.Add(pair.Key, new List<string>(pair.Value));
			}
			return clone;
		}

		/// <summary>Sets the <see cref="Method()"/> to <see cref="HttpMethod.Delete"/>.</summary>
		public KubernetesRequest Delete() => Method(HttpMethod.Delete);

		/// <summary>Sets the <see cref="Method()"/> to <see cref="HttpMethod.Get"/>.</summary>
		public KubernetesRequest Get() => Method(HttpMethod.Get);

#if !NET452 && !NETSTANDARD2_0
		/// <summary>Sets the <see cref="Method()"/> to <see cref="HttpMethod.Patch"/>.</summary>
		public KubernetesRequest Patch() => Method(HttpMethod.Patch);
#else
		/// <summary>Sets the <see cref="Method()"/> to PATCH.</summary>
		public KubernetesRequest Patch() => Method(new HttpMethod("PATCH"));
#endif

		/// <summary>Sets the <see cref="Method()"/> to <see cref="HttpMethod.Post"/>.</summary>
		public KubernetesRequest Post() => Method(HttpMethod.Post);

		/// <summary>Sets the <see cref="Method()"/> to <see cref="HttpMethod.Put"/>.</summary>
		public KubernetesRequest Put() => Method(HttpMethod.Put);

		/// <summary>Sets the value of the "dryRun" query-string parameter, as a boolean.</summary>
		public bool DryRun() => !string.IsNullOrEmpty(GetQuery("dryRun"));

		/// <summary>Sets the value of the "dryRun" query-string parameter to "All" or removes it.</summary>
		public KubernetesRequest DryRun(bool dryRun) => SetQuery("dryRun", dryRun ? "All" : null);

		/// <summary>Opens a stream to the "exec" subresource to execute a command and sends the body, if any, as standard input.</summary>
		/// <param name="command">The command to execute</param>
		/// <param name="args">The arguments to the command, or null if there are no arguments. The default is null.</param>
		/// <param name="container">The container to execute the command on, or null or empty if there's only one container.
		/// The default is null.
		/// </param>
		/// <param name="stdout">A <see cref="Stream"/> to which the standard output stream should be copied, or null if the standard
		/// output stream is unwanted. The default is null.
		/// </param>
		/// <param name="stderr">A <see cref="Stream"/> to which the standard error stream should be copied, or null if the standard
		/// error stream is unwanted. The default is null.
		/// </param>
		/// <param name="tty">If true, the command will be executed with its inputs and outputs configured as a virtual TTY.
		/// The default is false.
		/// </param>
		/// <param name="timeoutMs">A timeout, in milliseconds, after which the command will be aborted, or <see cref="Timeout.Infinite"/>
		/// if the command should not time out. The default is <see cref="Timeout.Infinite"/>.
		/// </param>
		/// <param name="throwOnFailure">If true, an exception will be thrown if the command terminates with a non-zero exit code
		/// or otherwise fails with an error. The default is true.
		/// </param>
		/// <param name="cancelToken">A <see cref="CancellationToken"/> that can be used to cancel the execution</param>
		/// <returns>Returns a <see cref="V1Status"/> indicating the result of the command execution. The <see cref="V1Status.Code"/>
		/// property will contain the exit code, or -1 if the exit code is unknown.
		/// </returns>
		/// <exception cref="KubernetesException">Thrown if <paramref name="throwOnFailure"/> is true and the command fails</exception>
		/// <exception cref="TimeoutException">Thrown if the command times out</exception>
		public async Task<V1Status> ExecCommandAsync(
			string command, string[] args = null, string container = null, Stream stdout = null, Stream stderr = null,
			bool tty = false, int timeoutMs = Timeout.Infinite, bool throwOnFailure = true, CancellationToken cancelToken = default)
		{
			if(string.IsNullOrEmpty(command)) throw new ArgumentNullException(nameof(command));
			if(_watchVersion != null) throw new InvalidOperationException("WatchVersion is incompatible with exec.");
			var req = Clone().Subresource("exec").Post().SetQuery("container", NormalizeEmpty(container))
				.SetQuery("stdin", _body != null ? "1" : "0").SetQuery("stdout", stdout != null ? "1" : "0")
				.SetQuery("stderr", stderr != null ? "1" : "0").SetQuery("tty", tty ? "1" : "0")
				.SetQuery("command", command).AddQuery("command", args);
			// NOTE: a bug in Kubernetes prevents protocol negotiation when sending multiple versions, so just send one.
			// v4 is implemented in all supported Kubernetes versions anyway. see https://github.com/kubernetes/kubernetes/issues/89849
			req.SetHeader("X-Stream-Protocol-Version", "v4.channel.k8s.io");

			Stream stdin = null; // if there's a body, get a stream for it
			if(_body != null)
			{
				stdin = _body as Stream;
				if(stdin == null)
				{
					stdin = new MemoryStream(_body as byte[] ??
						Encoding.UTF8.GetBytes(_body as string ?? FluentExtensions.Serialize(_body)));
				}
			}

			// open the SPDY connection and execute the command. we use SPDY because of a flaw in the Kubernetes web sockets protocol:
			// https://github.com/kubernetes/kubernetes/issues/89899
			var (spdyConn, headers) = await req.OpenSPDYAsync(cancelToken).ConfigureAwait(false);
			V1Status status = await new SPDYExec(spdyConn, headers, stdin, stdout, stderr).RunAsync(timeoutMs, cancelToken).ConfigureAwait(false);
			if(throwOnFailure && status.Status == "Failure") throw new KubernetesException(status);
			return status;
		}

		/// <summary>Executes the request and returns a <see cref="KubernetesResponse"/>. The request can be executed multiple times,
		/// and can be executed multiple times in parallel.
		/// </summary>
		public Task<KubernetesResponse> ExecuteAsync(CancellationToken cancelToken = default) => ExecuteAsync(false, cancelToken);

		/// <summary>Executes the request and returns a <see cref="KubernetesResponse"/>. The request can be executed multiple times,
		/// and can be executed multiple times in parallel.
		/// </summary>
		/// <param name="throwIfFailed">If true and the response is an error other than 404 Not Found, an exception will be thrown.
		/// Otherwise, the response will be returned, whatever it is. The default is false.
		/// </param>
		/// <param name="cancelToken">A <see cref="CancellationToken"/> that can be used to cancel the request</param>
		/// <exception cref="KubernetesException">Thrown if <paramref name="throwIfFailed"/> was true and the response was any error
		/// besides 404 Not Found.
		/// </exception>
		public async Task<KubernetesResponse> ExecuteAsync(bool throwIfFailed, CancellationToken cancelToken = default)
		{
			cancelToken.ThrowIfCancellationRequested();
			HttpRequestMessage req = await CreateRequestMessage(cancelToken).ConfigureAwait(false);
			// requests like watches may not send a body immediately, so return as soon as we've got the response headers
			var completion = _streamResponse || _watchVersion != null ?
				HttpCompletionOption.ResponseHeadersRead : HttpCompletionOption.ResponseContentRead;
			var resp = new KubernetesResponse(await client.SendAsync(req, completion, cancelToken).ConfigureAwait(false));
			if(throwIfFailed && resp.IsError && !resp.IsNotFound)
			{
				throw new KubernetesException(await resp.GetStatusAsync().ConfigureAwait(false));
			}
			return resp;
		}

		/// <summary>Executes the request and returns the deserialized response body (or the default value of type
		/// <typeparamref name="T"/> if the response was 404 Not Found).
		/// </summary>
		/// <exception cref="KubernetesException">Thrown if the response was any error besides 404 Not Found.</exception>
		public Task<T> ExecuteAsync<T>(CancellationToken cancelToken = default) => ExecuteAsync<T>(false, cancelToken);

		/// <summary>Executes the request and returns the deserialized response body.</summary>
		/// <param name="throwIfMissing">If true and the response is 404 Not Found, an exception will be thrown. If false, the default
		/// value of type <typeparamref name="T"/> will be returned in that case. The default is false.
		/// </param>
		/// <param name="cancelToken">A <see cref="CancellationToken"/> that can be used to cancel the request</param>
		/// <exception cref="KubernetesException">Thrown if the response was any error besides 404 Not Found, or if the response was
		/// 404 Not Found and <paramref name="throwIfMissing"/> was true.
		/// </exception>
		public async Task<T> ExecuteAsync<T>(bool throwIfMissing, CancellationToken cancelToken = default)
		{
			if(_watchVersion != null) throw new InvalidOperationException("Watch requests cannot be deserialized all at once.");
			cancelToken.ThrowIfCancellationRequested();
			HttpRequestMessage msg = await CreateRequestMessage(cancelToken).ConfigureAwait(false);
			KubernetesResponse resp = new KubernetesResponse(await client.SendAsync(msg, cancelToken).ConfigureAwait(false));
			if(resp.IsNotFound && !throwIfMissing) return default(T);
			else if(resp.IsError) throw new KubernetesException(await resp.GetStatusAsync().ConfigureAwait(false));
			else return await resp.GetBodyAsync<T>().ConfigureAwait(false);
		}

		/// <summary>Gets the "fieldManager" query-string parameter, or null if there is no field manager.</summary>
		public string FieldManager() => NormalizeEmpty(GetQuery("fieldManager"));

		/// <summary>Sets the "fieldManager" query-string parameter, or removes it if the value is null or empty.</summary>
		public KubernetesRequest FieldManager(string manager) =>
			SetQuery("fieldManager", !string.IsNullOrEmpty(manager) ? manager : null);

		/// <summary>Gets the "fieldSelector" query-string parameter, or null if there is no field selector.</summary>
		public string FieldSelector() => NormalizeEmpty(GetQuery("fieldSelector"));

		/// <summary>Sets the "fieldSelector" query-string parameter, or removes it if the selector is null or empty.</summary>
		public KubernetesRequest FieldSelector(string selector) =>
			SetQuery("fieldSelector", !string.IsNullOrEmpty(selector) ? selector : null);

		/// <summary>Gets the value of the named custom header, or null if it doesn't exist.</summary>
		/// <exception cref="InvalidOperationException">Thrown if there are multiple custom headers with the given name</exception>
		public string GetHeader(string key)
		{
			List<string> values = null;
			if(headers != null) headers.TryGetValue(key, out values);
			return values == null || values.Count == 0 ? null : values.Count == 1 ? values[0] :
				throw new InvalidOperationException($"There are multiple values for the header named '{key}'.");
		}

		/// <summary>Gets the values of the named custom header, or null if it has no values.</summary>
		/// <remarks>The returned collection, if not null, can be mutated to change the set of values.</remarks>
		public List<string> GetHeaderValues(string key)
		{
			List<string> values = null;
			if(headers != null) headers.TryGetValue(key, out values);
			return values;
		}

		/// <summary>Gets the value of the named query-string parameter, or null if it doesn't exist.</summary>
		/// <exception cref="InvalidOperationException">Thrown if there are multiple query-string parameters with the given name</exception>
		public string GetQuery(string key)
		{
			List<string> values = GetQueryValues(key);
			return values == null || values.Count == 0 ? null : values.Count == 1 ? values[0] :
				throw new InvalidOperationException($"There are multiple query-string parameters named '{key}'.");
		}

		/// <summary>Gets the values of the named query-string parameter, or null if it has no values.</summary>
		/// <remarks>The returned collection, if not null, can be mutated to change the set of values.</remarks>
		public List<string> GetQueryValues(string key)
		{
			List<string> values = null;
			if(query != null) query.TryGetValue(key, out values);
			return values;
		}

		/// <summary>Gets the Kubernetes API group to use, or null or empty to use the default, which is the core API group
		/// unless a <see cref="RawUri(string)"/> is given.
		/// </summary>
		public string Group() => _group;

		/// <summary>Sets the Kubernetes API group to use, or null or empty to use the default, which is the core API group
		/// unless a <see cref="RawUri(string)"/> is given.
		/// </summary>
		public KubernetesRequest Group(string group) { _group = NormalizeEmpty(group); return this; }

		/// <summary>Attempts to set the <see cref="Group()"/>, <see cref="Version()"/>, and <see cref="Type()"/> based on an object.</summary>
		/// <remarks>The method calls <see cref="GVK(Type)"/> with the object's type. Then, if <see cref="IKubernetesObject.ApiVersion"/>
		/// is set, it will override <see cref="Group()"/> and <see cref="Version()"/>.
		/// </remarks>
		public KubernetesRequest GVK(IKubernetesObject obj)
		{
			if(obj == null) throw new ArgumentNullException();
			GVK(obj.GetType());
			if(!string.IsNullOrEmpty(obj.ApiVersion)) // if the object has an API version set, use it...
			{
				int slash = obj.ApiVersion.IndexOf('/'); // the ApiVersion field is in the form "version" or "group/version"
				Group(slash >= 0 ? obj.ApiVersion.Substring(0, slash) : null).Version(obj.ApiVersion.Substring(slash+1));
			}
			return this;
		}

		/// <summary>Attempts to set the <see cref="Group()"/>, <see cref="Version()"/>, and <see cref="Type()"/> based on a Kubernetes
		/// API version (including the API group) and kind. The method uses heuristics and may not work in all cases.
		/// </summary>
		public KubernetesRequest GVK(string apiVersion, string kind)
		{
			string group = null, version = apiVersion;
			if(!string.IsNullOrEmpty(apiVersion))
			{
				int slash = apiVersion.IndexOf('/');
				if(slash >= 0) (group, version) = (apiVersion.Substring(0, slash), apiVersion.Substring(slash+1));
			}
			return GVK(group, version, kind);
		}

		/// <summary>Attempts to set the <see cref="Group()"/>, <see cref="Version()"/>, and <see cref="Type()"/> based on a Kubernetes
		/// group, version, and kind. The method uses heuristics and may not work in all cases.
		/// </summary>
		public KubernetesRequest GVK(string group, string version, string kind) =>
			Group(!string.IsNullOrEmpty(group) ? group : null).Version(!string.IsNullOrEmpty(version) ? version : null)
				.Type(KubernetesScheme.GuessPath(kind));

		/// <summary>Attempts to set the <see cref="Group()"/>, <see cref="Version()"/>, and <see cref="Type()"/> based on a type of object,
		/// such as <see cref="k8s.Models.V1Pod"/>.
		/// </summary>
		public KubernetesRequest GVK(Type type)
		{
			if(type == null) throw new ArgumentNullException(nameof(type));
			_scheme.GetGVK(type, out string group, out string version, out string kind, out string path);
			return Group(NormalizeEmpty(group)).Version(version).Type(path);
		}

		/// <summary>Attempts to set the <see cref="Group()"/>, <see cref="Version()"/>, and <see cref="Type()"/> based on a type of object,
		/// such as <see cref="k8s.Models.V1Pod"/>.
		/// </summary>
		public KubernetesRequest GVK<T>() => GVK(typeof(T));

		/// <summary>Gets the "labelSelector" query-string parameter, or null if there is no label selector.</summary>
		public string LabelSelector() => NormalizeEmpty(GetQuery("labelSelector"));

		/// <summary>Sets the "labelSelector" query-string parameter, or removes it if the selecor is null or empty.</summary>
		public KubernetesRequest LabelSelector(string selector) =>
			SetQuery("labelSelector", !string.IsNullOrEmpty(selector) ? selector : null);

		/// <summary>Gets the value of the Content-Type header, or null to use the default of application/json.</summary>
		public string MediaType() => _mediaType;

		/// <summary>Sets the value of the Content-Type header, not including any parameters, or null or empty to use the default
		/// of application/json. The header value will only be used if a <see cref="Body(object)"/> is supplied.
		/// </summary>
		public KubernetesRequest MediaType(string mediaType) { _mediaType = NormalizeEmpty(mediaType); return this; }

		/// <summary>Gets the <see cref="HttpMethod"/> to use.</summary>
		public HttpMethod Method() => _method ?? HttpMethod.Get;

		/// <summary>Sets the <see cref="HttpMethod"/> to use, or null to use the default of <see cref="HttpMethod.Get"/>.</summary>
		public KubernetesRequest Method(HttpMethod method) { _method = method; return this; }

		/// <summary>Gets the name of the top-level Kubernetes resource to access.</summary>
		public string Name() => _name;

		/// <summary>Sets the name of the top-level Kubernetes resource to access, or null or empty to not access a specific object.</summary>
		public KubernetesRequest Name(string name) { _name = NormalizeEmpty(name); return this; }

		/// <summary>Gets the Kubernetes namespace to access.</summary>
		public string Namespace() => _ns;

		/// <summary>Sets the Kubernetes namespace to access, or null or empty to not access a namespaced object.</summary>
		public KubernetesRequest Namespace(string ns) { _ns = NormalizeEmpty(ns); return this; }

		/// <summary>Gets whether to use the old-style Kubernetes watch (e.g. /api/v1/watch/...) rather than the new-style watch.</summary>
		public bool OldStyleWatch() => _oldStyleWatch;

		/// <summary>Sets whether to use the old-style Kubernetes watch (e.g. /api/v1/watch/...) rather than the new-style watch.</summary>
		/// <remarks>If set to false (the default), Kubernetes requires you to use a list request combined with a field selector to watch
		/// a single item. If set to true, you can watch an item with a GET request to the specific item. If used with the the
		/// <see cref="Watch{T}"/> class, it's generally not necessary to set this property because the watch class will usually apply a
		/// suitable default.
		/// </remarks>
		public KubernetesRequest OldStyleWatch(bool value) { _oldStyleWatch = value; return this; }

		/// <summary>Opens a <see cref="SPDYConnection"/> to the resource described by the request, but does not send the body.</summary>
		/// <returns>Returns the <see cref="SPDYConnection"/> and the response headers.</returns>
		/// <exception cref="KubernetesException">Thrown if the request fails or cannot be upgraded</exception>
		public async Task<ValueTuple<SPDYConnection, HttpResponseHeaders>> OpenSPDYAsync(CancellationToken cancelToken = default)
		{
			cancelToken.ThrowIfCancellationRequested();
			var req = Clone().Accept("*/*").SetHeader("Connection", "Upgrade").SetHeader("Upgrade", "SPDY/3.1").Body(null).StreamResponse(true);
			HttpRequestMessage reqMsg = await req.CreateRequestMessage(cancelToken).ConfigureAwait(false);
			HttpResponseMessage respMsg = await client.SendAsync(reqMsg, HttpCompletionOption.ResponseHeadersRead, cancelToken).ConfigureAwait(false);
			if(respMsg.StatusCode != HttpStatusCode.SwitchingProtocols)
			{
				throw new KubernetesException(new V1Status()
				{
					Status = "Failure", Code = (int)respMsg.StatusCode, Reason = respMsg.StatusCode.ToString(),
					Message = "Unable to upgrade to SPDY/3.1 connection. " + await respMsg.Content.ReadAsStringAsync().ConfigureAwait(false)
				});
			}

			Stream stream = await respMsg.Content.ReadAsStreamAsync().ConfigureAwait(false);
#if NET452
			if(!stream.CanWrite) stream = new RawHttpStream(stream); // if it's writable, assume we can use it. otherwise, try to get the raw stream
#endif
			return (new SPDYConnection(stream, "3.1"), respMsg.Headers);
		}

#if NETCOREAPP2_1 || NETSTANDARD2_1
		/// <summary>Opens a <see cref="WebSocket"/> to the resource described by the request, but does not send the body.</summary>
		/// <returns>Returns the <see cref="WebSocket"/> and the response headers.</returns>
		/// <exception cref="KubernetesException">Thrown if the request fails or cannot be upgraded</exception>
		public async Task<ValueTuple<WebSocket, HttpResponseHeaders>> OpenWebSocketAsync(string subprotocol = null, CancellationToken cancelToken = default)
		{
			cancelToken.ThrowIfCancellationRequested();
			var req = Clone().Accept("*/*").SetHeader("Connection", "Upgrade").SetHeader("Upgrade", "websocket").Body(null).StreamResponse(true);
			HttpRequestMessage reqMsg = await req.CreateRequestMessage(cancelToken).ConfigureAwait(false);
			HttpResponseMessage respMsg = await client.SendAsync(reqMsg, HttpCompletionOption.ResponseHeadersRead, cancelToken).ConfigureAwait(false);
			if(respMsg.StatusCode != HttpStatusCode.SwitchingProtocols)
			{
				throw new KubernetesException(new V1Status() {
					Status = "Failure", Code = (int)respMsg.StatusCode, Reason = respMsg.StatusCode.ToString(),
					Message = "Unable to upgrade to web socket connection. " + await respMsg.Content.ReadAsStringAsync().ConfigureAwait(false) });
			}
			Stream stream = await respMsg.Content.ReadAsStreamAsync().ConfigureAwait(false);
			return (WebSocket.CreateFromStream(stream, false, subprotocol, WebSocket.DefaultKeepAliveInterval), respMsg.Headers);
		}
#endif

		/// <summary>Gets the raw URL to access, relative to the configured Kubernetes host and not including the query string, or
		/// null if the URL will be constructed piecemeal based on the other properties.
		/// </summary>
		public string RawUri() => _rawUri;

		/// <summary>Sets the raw URL to access, relative to the configured Kubernetes host and not including the query string, or
		/// null or empty to construct the URI piecemeal based on the other properties. The URI must begin with a slash.
		/// </summary>
		public KubernetesRequest RawUri(string uri)
		{
			uri = NormalizeEmpty(uri);
			if(uri != null && uri[0] != '/') throw new ArgumentException("The URI must begin with a slash.");
			_rawUri = uri;
			return this;
		}

		/// <summary>Performs an atomic get-modify-replace operation, using the GET method to read the object and the PUT method to
		/// replace it.
		/// </summary>
		/// <param name="update">A function that modifies the resource, returning true if any changes were made and false if not</param>
		/// <param name="throwIfMissing">If true, an exception will be thrown if the object doesn't exist. If false, null will be
		/// returned in that case.
		/// </param>
		/// <param name="cancelToken">A <see cref="CancellationToken"/> that can be used to cancel the request</param>
		public Task<T> ReplaceAsync<T>(Func<T, bool> update, bool throwIfMissing = false, CancellationToken cancelToken = default)
			where T : class, IMetadata<V1ObjectMeta> => ReplaceAsync<T>(null, update, throwIfMissing, cancelToken);

		/// <summary>Performs an atomic get-modify-replace operation, using the GET method to read the object and the PUT method to
		/// replace it.
		/// </summary>
		/// <param name="update">A function that modifies the resource, returning true if any changes were made and false if not</param>
		/// <param name="throwIfMissing">If true, an exception will be thrown if the object doesn't exist. If false, null will be
		/// returned in that case.
		/// </param>
		/// <param name="cancelToken">A <see cref="CancellationToken"/> that can be used to cancel the request</param>
		public Task<T> ReplaceAsync<T>(
			Func<T, CancellationToken, Task<bool>> update, bool throwIfMissing = false, CancellationToken cancelToken = default)
			where T : class, IMetadata<V1ObjectMeta> => ReplaceAsync<T>(null, update, throwIfMissing, cancelToken);

		/// <summary>Performs an atomic get-modify-replace operation, using the GET method to read the object and the PUT method to
		/// replace it.
		/// </summary>
		/// <param name="obj">The initial value of the resource, or null if it should be retrieved with a GET request</param>
		/// <param name="modify">A function that modifies the resource, returning true if any changes were made and false if not</param>
		/// <param name="throwIfMissing">If true, an exception will be thrown if the object doesn't exist. If false, null will be
		/// returned in that case.
		/// </param>
		/// <param name="cancelToken">A <see cref="CancellationToken"/> that can be used to cancel the request</param>
		public Task<T> ReplaceAsync<T>(T obj, Func<T, bool> modify, bool throwIfMissing = false, CancellationToken cancelToken = default)
			where T : class
		{
			if(modify == null) throw new ArgumentNullException(nameof(modify));
			return ReplaceAsync(obj, (o, ct) => Task.FromResult(modify(o)), throwIfMissing, cancelToken);
		}

		/// <summary>Performs an atomic get-modify-replace operation, using the GET method to read the object and the PUT method to
		/// replace it.
		/// </summary>
		/// <param name="obj">The initial value of the resource, or null if it should be retrieved with a GET request</param>
		/// <param name="modify">A function that modifies the resource, returning true if any changes were made and false if not</param>
		/// <param name="throwIfMissing">If true, an exception will be thrown if the object doesn't exist. If false, null will be
		/// returned in that case.
		/// </param>
		/// <param name="cancelToken">A <see cref="CancellationToken"/> that can be used to cancel the request</param>
		/// <exception cref="KubernetesException">Thrown if a request fails</exception>
		public async Task<T> ReplaceAsync<T>(
			T obj, Func<T, CancellationToken, Task<bool>> modify, bool throwIfMissing = false, CancellationToken cancelToken = default)
			where T : class
		{
			if(modify == null) throw new ArgumentNullException(nameof(modify));
			if(_watchVersion != null) throw new InvalidOperationException("Watches cannot be updated.");
			KubernetesRequest req = Clone();
			while(true)
			{
				if(obj == null) // if we need to load the resource...
				{
					cancelToken.ThrowIfCancellationRequested(); // load it with a GET request
					obj = await req.Get().Body(null).ExecuteAsync<T>(throwIfMissing, cancelToken).ConfigureAwait(false);
				}
				cancelToken.ThrowIfCancellationRequested();
				// if the resource is missing or no changes are needed, return it as-is. otherwise, update it with a PUT request
				if(obj == null || !await modify(obj, cancelToken).ConfigureAwait(false)) return obj;
				KubernetesResponse resp = await req.Put().Body(obj).ExecuteAsync(cancelToken).ConfigureAwait(false);
				if(resp.StatusCode != HttpStatusCode.Conflict) // if there was no conflict, return the result
				{
					if(resp.IsNotFound && !throwIfMissing) return null;
					else if(resp.IsError) throw new KubernetesException(await resp.GetStatusAsync().ConfigureAwait(false));
					else return await resp.GetBodyAsync<T>().ConfigureAwait(false);
				}
				obj = null; // otherwise, there was a conflict, so reload the item
			}
		}

		/// <summary>Gets the <see cref="KubernetesScheme"/> used to map types to their Kubernetes groups, version, and kinds.</summary>
		public KubernetesScheme Scheme() => _scheme;

		/// <summary>Sets the <see cref="KubernetesScheme"/> used to map types to their Kubernetes groups, version, and kinds, or null to
		/// use the <see cref="KubernetesScheme.Default"/> scheme.
		/// </summary>
		public KubernetesRequest Scheme(KubernetesScheme scheme) { _scheme = scheme ?? KubernetesScheme.Default; return this; }

		/// <summary>Attempts to set the <see cref="Group()"/>, <see cref="Version()"/>, <see cref="Type()"/>, <see cref="Namespace()"/>,
		/// <see cref="Name()"/>, and optionally the <see cref="Body()"/> based on the given object.
		/// </summary>
		/// <remarks>If the object implements <see cref="IMetadata{T}"/> of <see cref="V1ObjectMeta"/>, it will be used to set the
		/// <see cref="Name()"/> and <see cref="Namespace()"/>. The <see cref="Name()"/> will be set if <see cref="V1ObjectMeta.Uid"/>
		/// is set (on the assumption that you're accessing an existing object), and cleared it's clear (on the assumption that you're
		/// creating a new object and want to POST to its container).
		/// </remarks>
		public KubernetesRequest Set(IKubernetesObject obj, bool setBody = true)
		{
			GVK(obj);
			if(setBody) Body(obj);
			var kobj = obj as IMetadata<V1ObjectMeta>;
			if(kobj != null) Namespace(kobj.Namespace()).Name(!string.IsNullOrEmpty(kobj.Uid()) ? kobj.Name() : null);
			return this;
		}

		/// <summary>Sets a custom header value, or deletes it if the value is null.</summary>
		public KubernetesRequest SetHeader(string headerName, string value) => Set(ref headers, CheckHeaderName(headerName), value);

		/// <summary>Sets a query-string value, or deletes it if the value is null.</summary>
		public KubernetesRequest SetQuery(string key, string value) => Set(ref query, key, value);

		/// <summary>Sets the <see cref="Subresource()"/> to "status", to get or set a resource's status.</summary>
		public KubernetesRequest Status() => Subresource("status");

		/// <summary>Gets whether the response must be streamed. If true, the response will be returned from
		/// <see cref="ExecuteAsync(CancellationToken)"/> as soon as the headers are read and you will have to dispose the response.
		/// Otherwise, the entire response will be downloaded before <see cref="ExecuteAsync(CancellationToken)"/> returns, and you will
		/// not have to dispose it. Note that regardless of the value of this property, the response is always streamed when
		/// <see cref="WatchVersion()"/> is not null.
		/// </summary>
		public bool StreamResponse() => _streamResponse;

		/// <summary>Sets whether the response must be streamed. If true, the response will be returned from
		/// <see cref="ExecuteAsync(CancellationToken)"/> as soon as the headers are read and you will have to dispose the response.
		/// Otherwise, the entire response will be downloaded before <see cref="ExecuteAsync(CancellationToken)"/> returns, and you will
		/// not have to dispose it. Note that regardless of the value of this property, the response is always streamed when
		/// <see cref="WatchVersion()"/> is not null.
		/// </summary>
		public KubernetesRequest StreamResponse(bool stream) { _streamResponse = stream; return this; }

		/// <summary>Gets the URL-encoded subresource to access, or null to not access a subresource.</summary>
		public string Subresource() => _subresource;

		/// <summary>Sets the subresource to access, or null or empty to not access a subresource. The value must be URL-encoded
		/// already if necessary.
		/// </summary>
		public KubernetesRequest Subresource(string subresource) { _subresource = NormalizeEmpty(subresource); return this; }

		/// <summary>Sets the value of the <see cref="Subresource(string)"/> by joining together one or more path segments. The
		/// segments will be URL-escaped (and so should not be URL-escaped already).
		/// </summary>
		public KubernetesRequest Subresources(params string[] subresources) =>
			Subresource(subresources != null && subresources.Length != 0 ?
				string.Join("/", subresources.Select(Uri.EscapeDataString)) : null);

		/// <inheritdoc/>
		public override string ToString() => Method().Method + " " + GetRequestUri();

		/// <summary>Creates a <see cref="Watch{T}"/> that watches for changes to the item or list of items represented by this request.</summary>
		/// <typeparam name="T">The type of item to watch for changes to</typeparam>
		/// <param name="initialVersion">The initial version to watch for. This will be used to set the <see cref="WatchVersion(string)"/>
		/// before creating the watch. If null or empty, the watch will start from the current version. The default is null.
		/// </param>
		/// <param name="isListWatch">Indicates whether the request will return a list of possibly multiple items (true) or only a single item
		/// (false). If null, the default value will be false if <see cref="Name()"/> is set and true if <see cref="Name()"/> is not set.
		/// </param>
		public Watch<T> ToWatch<T>(string initialVersion = null, bool? isListWatch = null) where T : IKubernetesObject, IMetadata<V1ObjectMeta>
			=> new Watch<T>(this, initialVersion, isListWatch);

		/// <summary>Gets the resource type access (e.g. "pods").</summary>
		public string Type() => _type;

		/// <summary>Sets the resource type access (e.g. "pods").</summary>
		public KubernetesRequest Type(string type) { _type = NormalizeEmpty(type); return this; }

		/// <summary>Gets the Kubernetes API version to use, or null to use the default, which is "v1"
		/// unless a <see cref="RawUri()"/> is given.
		/// </summary>
		public string Version() => _version;

		/// <summary>Sets the Kubernetes API version to use, or null or empty to use the default, which is "v1"
		/// unless a <see cref="RawUri()"/> is given.
		/// </summary>
		public KubernetesRequest Version(string version) { _version = NormalizeEmpty(version); return this; }

		/// <summary>Gets the resource version to use when watching a resource, or empty to watch the current version, or null
		/// to not execute a watch.
		/// </summary>
		public string WatchVersion() => _watchVersion;

		/// <summary>Sets the resource version to use when watching a resource, or empty to watch the current version, or null to not
		/// execute a watch. The default is null. When set, the response is always streamed (as though <see cref="StreamResponse()"/>
		/// was true).
		/// </summary>
		public KubernetesRequest WatchVersion(string resourceVersion) { _watchVersion = resourceVersion; return this; }

#if NET452
		#region RawHttpStream
		/// <summary>A stream that provides access to a writable HTTP stream when given a read-only response stream.</summary>
		// HACK: .NET Framework's response stream is not only read-only, it's empty when used with an upgraded connection. so we have
		// this hacky class to extract a writable stream from it via reflection
		sealed class RawHttpStream : Stream
		{
			public RawHttpStream(Stream responseStream)
			{
				this.responseStream = responseStream; // keep a reference to the response stream to prevent it from being garbage-collected
				const BindingFlags Flags = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;
				Type baseType = responseStream.GetType().BaseType;
				object o = (baseType.GetField("innerStream", Flags) ?? baseType.GetField("_innerStream"))?.GetValue(responseStream); // ReadOnlyStream -> WebExceptionWrapperStream
				o = o?.GetType().BaseType.GetField("innerStream", Flags)?.GetValue(o); // WebExceptionWrapperStream -> ConnectStream
				o = o?.GetType().GetProperty("Connection", Flags)?.GetValue(o); // ConnectStream -> Connection
				rawStream = (Stream)o?.GetType().GetProperty("NetworkStream", Flags)?.GetValue(o); // Connection -> NetworkStream
				if(rawStream == null) throw new NotSupportedException("Unable to retrieve the raw connection stream.");
			}

			public override bool CanRead => rawStream.CanRead; // why oh why doesn't a delegating stream exist in the framework already?
			public override bool CanSeek => rawStream.CanSeek;
			public override bool CanTimeout => rawStream.CanTimeout;
			public override bool CanWrite => rawStream.CanWrite;
			public override long Length => rawStream.Length;
			public override long Position { get => rawStream.Position; set => rawStream.Position = value; }
			public override int ReadTimeout { get => rawStream.ReadTimeout; set => rawStream.ReadTimeout = value; }
			public override int WriteTimeout { get => rawStream.WriteTimeout; set => rawStream.WriteTimeout = value; }

			public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state) =>
				rawStream.BeginRead(buffer, offset, count, callback, state);

			public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state) =>
				rawStream.BeginWrite(buffer, offset, count, callback, state);

			public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken) =>
				rawStream.CopyToAsync(destination, bufferSize, cancellationToken);

			public override int EndRead(IAsyncResult asyncResult) => rawStream.EndRead(asyncResult);
			public override void EndWrite(IAsyncResult asyncResult) => rawStream.EndWrite(asyncResult);
			public override void Flush() => rawStream.Flush();
			public override Task FlushAsync(CancellationToken cancellationToken) => rawStream.FlushAsync(cancellationToken);
			public override int Read(byte[] buffer, int offset, int count) => rawStream.Read(buffer, offset, count);
			public override int ReadByte() => rawStream.ReadByte();
			public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
				rawStream.ReadAsync(buffer, offset, count, cancellationToken);

			public override long Seek(long offset, SeekOrigin origin) => rawStream.Seek(offset, origin);
			public override void SetLength(long value) => rawStream.SetLength(value);
			public override void Write(byte[] buffer, int offset, int count) => rawStream.Write(buffer, offset, count);
			public override void WriteByte(byte value) => rawStream.WriteByte(value);
			public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
				rawStream.WriteAsync(buffer, offset, count, cancellationToken);

			protected override void Dispose(bool disposing)
			{
				base.Dispose(disposing);
				responseStream.Dispose();
			}

			readonly Stream responseStream, rawStream;
		}
		#endregion
#endif

		/// <summary>Adds a value to the query string or headers.</summary>
		KubernetesRequest Add(ref Dictionary<string, List<string>> dict, string key, string value)
		{
			if(string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
			if(dict == null) dict = new Dictionary<string, List<string>>();
			if(!dict.TryGetValue(key, out List<string> values)) dict[key] = values = new List<string>();
			values.Add(value);
			return this;
		}

		/// <summary>Adds a value to the query string or headers.</summary>
		KubernetesRequest Add(ref Dictionary<string, List<string>> dict, string key, IEnumerable<string> values)
		{
			if(string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
			if(values != null)
			{
				if(dict == null) dict = new Dictionary<string, List<string>>();
				if(!dict.TryGetValue(key, out List<string> list)) dict[key] = list = new List<string>();
				list.AddRange(values);
			}
			return this;
		}

		/// <summary>Sets a value in the query string or headers.</summary>
		KubernetesRequest Set(ref Dictionary<string, List<string>> dict, string key, string value)
		{
			if(string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
			dict = dict ?? new Dictionary<string, List<string>>();
			if(!dict.TryGetValue(key, out List<string> values)) dict[key] = values = new List<string>();
			values.Clear();
			values.Add(value);
			return this;
		}

		/// <summary>Creates an <see cref="HttpRequestMessage"/> representing the current request.</summary>
#if NETCOREAPP2_1
		async ValueTask<HttpRequestMessage> CreateRequestMessage(CancellationToken cancelToken)
#else
		async Task<HttpRequestMessage> CreateRequestMessage(CancellationToken cancelToken)
#endif
		{
			var req = new HttpRequestMessage(Method(), GetRequestUri());
			if(credentials != null) await credentials.ProcessHttpRequestAsync(req, cancelToken).ConfigureAwait(false);

			// add the headers
			if(_accept != null) req.Headers.Add("Accept", _accept);
			List<KeyValuePair<string, List<string>>> contentHeaders = null;
			if(headers != null && headers.Count != 0) // add custom headers
			{
				contentHeaders = new List<KeyValuePair<string, List<string>>>(); // some headers must be added to .Content.Headers. track them
				foreach(KeyValuePair<string, List<string>> pair in headers)
				{
					if(!req.Headers.TryAddWithoutValidation(pair.Key, pair.Value)) // if it's not legal to set this header on the request...
					{
						contentHeaders.Add(new KeyValuePair<string, List<string>>(pair.Key, pair.Value)); // assume we should set it on the content
						break;
					}
				}
			}

			// add the body, if any
			if(_body != null)
			{
				if(_body is byte[] bytes) req.Content = new ByteArrayContent(bytes);
				else if(_body is Stream stream) req.Content = new StreamContent(stream);
				else req.Content = new StringContent(_body as string ?? FluentExtensions.Serialize(_body), Encoding.UTF8);
				req.Content.Headers.ContentType = new MediaTypeHeaderValue(_mediaType ?? "application/json") { CharSet = "UTF-8" };
				if(contentHeaders != null && contentHeaders.Count != 0) // go through the headers we couldn't set on the request
				{
					foreach(KeyValuePair<string, List<string>> pair in contentHeaders)
					{
						if(!req.Content.Headers.TryAddWithoutValidation(pair.Key, pair.Value)) // if we can't set it on the content either...
						{
							throw new InvalidOperationException($"{pair.Value} is a response header and cannot be set on the request.");
						}
					}
				}
			}
			return req;
		}

		string GetRequestUri()
		{
			if(_rawUri != null && (_group ?? _name ?? _ns ?? _subresource ?? _type ?? _version) != null)
			{
				throw new InvalidOperationException("You cannot use both raw and piecemeal URIs.");
			}

			// construct the request URL
			var sb = new StringBuilder();
			sb.Append(baseUri);
			if(_rawUri != null) // if a raw URL was given, use it
			{
				if(sb[sb.Length-1] == '/') sb.Length--; // the raw URI starts with a slash, so ensure the base URI doesn't end with one
				sb.Append(_rawUri);
			}
			else // otherwise, construct it piecemeal
			{
				if(sb[sb.Length-1] != '/') sb.Append('/'); // ensure the base URI ends with a slash
				if(_group != null) sb.Append("apis/").Append(_group);
				else sb.Append("api");
				sb.Append('/').Append(_version ?? "v1");
				if(_oldStyleWatch) sb.Append("/watch");
				if(_ns != null) sb.Append("/namespaces/").Append(_ns);
				sb.Append('/').Append(_type);
				if(_name != null) sb.Append('/').Append(_name);
				if(_subresource != null) sb.Append('/').Append(_subresource);
			}
			bool firstParam = true;
			if(query != null) // then add the query string, if any
			{
				foreach(KeyValuePair<string, List<string>> pair in query)
				{
					string key = Uri.EscapeDataString(pair.Key);
					foreach(string value in pair.Value)
					{
						sb.Append(firstParam ? '?' : '&').Append(key).Append('=');
						if(!string.IsNullOrEmpty(value)) sb.Append(Uri.EscapeDataString(value));
						firstParam = false;
					}
				}
			}
			if(_watchVersion != null)
			{
				sb.Append(firstParam ? '?' : '&').Append("watch=1");
				if(_watchVersion.Length != 0) sb.Append("&resourceVersion=").Append(_watchVersion);
			}
			return sb.ToString();
		}

		object ICloneable.Clone() => Clone();

		readonly HttpClient client;
		readonly string baseUri;
		readonly ServiceClientCredentials credentials;
		Dictionary<string, List<string>> headers, query;
		string _accept = "application/json", _mediaType = "application/json";
		string _group, _name, _ns, _rawUri, _subresource, _type, _version, _watchVersion;
		object _body;
		HttpMethod _method;
		KubernetesScheme _scheme;
		bool _oldStyleWatch, _streamResponse;

		static string CheckHeaderName(string name)
		{
			if(name == "Accept" || name == "Content-Type")
			{
				throw new ArgumentException($"The {name} header must be set using the corresponding property.");
			}
			return name;
		}

		static string NormalizeEmpty(string value) => string.IsNullOrEmpty(value) ? null : value; // normalizes empty strings to null
	}
}
