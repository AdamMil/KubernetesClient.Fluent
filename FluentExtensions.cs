using System;
using System.Net.Http;
using k8s.Models;
using Newtonsoft.Json;

namespace k8s.Fluent
{
	/// <summary>Provides extension methods that implement a fluent interface for a <see cref="Kubernetes"/> client.</summary>
	public static class FluentExtensions
	{
		/// <summary>Creates a new Kubernetes object of the given type and sets its <see cref="IKubernetesObject.ApiVersion"/> and
		/// <see cref="IKubernetesObject.Kind"/>.
		/// </summary>
		/// <remarks>This method uses the <see cref="KubernetesScheme.Default"/> <see cref="KubernetesScheme"/>.</remarks>
		public static T New<T>(this IKubernetes client) where T : IKubernetesObject, new() => KubernetesScheme.Default.New<T>();

		/// <summary>Creates a new Kubernetes object of the given type and sets its <see cref="IKubernetesObject.ApiVersion"/>,
		/// <see cref="IKubernetesObject.Kind"/>, and <see cref="V1ObjectMeta.Name"/>.
		/// </summary>
		/// <remarks>This method uses the <see cref="KubernetesScheme.Default"/> <see cref="KubernetesScheme"/>.</remarks>
		public static T New<T>(this IKubernetes client, string name) where T : IKubernetesObject<V1ObjectMeta>, new() =>
			KubernetesScheme.Default.New<T>(name);

		/// <summary>Creates a new Kubernetes object of the given type and sets its <see cref="IKubernetesObject.ApiVersion"/>,
		/// <see cref="IKubernetesObject.Kind"/>, <see cref="V1ObjectMeta.NamespaceProperty"/>, and <see cref="V1ObjectMeta.Name"/>.
		/// </summary>
		/// <remarks>This method uses the <see cref="KubernetesScheme.Default"/> <see cref="KubernetesScheme"/>.</remarks>
		public static T New<T>(this IKubernetes client, string ns, string name) where T : IKubernetesObject<V1ObjectMeta>, new() =>
			KubernetesScheme.Default.New<T>(ns, name);

		/// <summary>Creates a new <see cref="KubernetesRequest"/> using the given <see cref="HttpMethod"/>
		/// (<see cref="HttpMethod.Get"/> by default).
		/// </summary>
		public static KubernetesRequest Request(this Kubernetes client, HttpMethod method = null) =>
			new KubernetesRequest(client).Method(method);

		/// <summary>Creates a new <see cref="KubernetesRequest"/> using the given <see cref="HttpMethod"/>
		/// and resource URI components.
		/// </summary>
		public static KubernetesRequest Request(this Kubernetes client, 
			HttpMethod method, string type = null, string ns = null, string name = null, string group = null, string version = null) =>
			new KubernetesRequest(client).Method(method).Group(group).Version(version).Type(type).Namespace(ns).Name(name);

		/// <summary>Creates a new <see cref="KubernetesRequest"/> to access the given type of object.</summary>
		public static KubernetesRequest Request(this Kubernetes client, Type type) => new KubernetesRequest(client).GVK(type);

		/// <summary>Creates a new <see cref="KubernetesRequest"/> to access the given type of object with an optional name and namespace.</summary>
		public static KubernetesRequest Request(this Kubernetes client, HttpMethod method, Type type, string ns = null, string name = null) =>
			Request(client, method).GVK(type).Namespace(ns).Name(name);

		/// <summary>Creates a new <see cref="KubernetesRequest"/> to access the given type of object with an optional name and namespace.</summary>
		public static KubernetesRequest Request<T>(this Kubernetes client, string ns = null, string name = null) =>
			Request(client, null, typeof(T), ns, name);

		/// <summary>Creates a new <see cref="KubernetesRequest"/> to access the given object.</summary>
		public static KubernetesRequest Request(this Kubernetes client, IKubernetesObject obj, bool setBody = true) =>
			new KubernetesRequest(client).Set(obj, setBody);

		/// <summary>Serializes an object using the <see cref="DefaultSerializer"/>.</summary>
		internal static object Deserialize(string json, Type type)
		{
			using(var reader = new JsonTextReader(new System.IO.StringReader(json))) return DefaultSerializer.Deserialize(reader, type);
		}

		/// <summary>Serializes an object using the <see cref="DefaultSerializer"/>.</summary>
		internal static string Serialize(object value)
		{
			var sw = new System.IO.StringWriter(new System.Text.StringBuilder(256), System.Globalization.CultureInfo.InvariantCulture);
			using(var writer = new JsonTextWriter(sw)) // do it the same way JsonConvert.SerializeObject does
			{
				writer.Formatting = DefaultSerializer.Formatting;
				DefaultSerializer.Serialize(writer, value, null);
			}
			return sw.ToString();
		}

		/// <summary>Gets the <see cref="JsonSerializerSettings"/> used to serialize and deserialize Kubernetes objects.</summary>
		internal static readonly JsonSerializer DefaultSerializer = JsonSerializer.Create(CreateSerializerSettings());

		/// <summary>Creates the JSON serializer settings used for serializing request bodies and deserializing responses.</summary>
		static JsonSerializerSettings CreateSerializerSettings()
		{
			var settings = new JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore };
			settings.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter());
			return settings;
		}
	}
}
