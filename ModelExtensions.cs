using System;
using k8s.Models;

namespace k8s.Fluent
{
	/// <summary>Adds convenient extensions for Kubernetes objects.</summary>
	public static class ModelExtensions
	{
		/// <summary>Adds an owner reference to the object. No attempt is made to ensure the reference is correct or fits with the
		/// other references.
		/// </summary>
		public static void AddOwnerReference(
			this IMetadata<V1ObjectMeta> obj, IKubernetesObject<V1ObjectMeta> owner, bool? controller = null, bool? blockDeletion = null) =>
			obj.AddOwnerReference(CreateOwnerReference(owner, controller, blockDeletion));

		/// <summary>Clones an <see cref="IKubernetesObject"/> by serializing and deserializing it.</summary>
		public static T Clone<T>(this T obj) where T : IKubernetesObject
		{
			if(obj == null) return default(T);
			var buffer = new System.Text.StringBuilder();
			using(var sw = new System.IO.StringWriter(buffer)) FluentExtensions.DefaultSerializer.Serialize(sw, obj);
			using(var sr = new System.IO.StringReader(buffer.ToString()))
			{
				return (T)FluentExtensions.DefaultSerializer.Deserialize(sr, obj.GetType());
			}
		}

		/// <summary>Creates a <see cref="V1ObjectReference"/> that refers to the given object.</summary>
		public static V1ObjectReference CreateObjectReference(this IKubernetesObject<V1ObjectMeta> obj)
		{
			if(obj == null) throw new ArgumentNullException(nameof(obj));
			string apiVersion = obj.ApiVersion, kind = obj.Kind; // default to using the API version and kind from the object
			if(string.IsNullOrEmpty(apiVersion) || string.IsNullOrEmpty(kind)) // but if either of them is missing...
			{
				KubernetesScheme.Default.GetVK(obj.GetType(), out apiVersion, out kind); // get it from the default scheme
			}
			return new V1ObjectReference()
			{
				ApiVersion = apiVersion, Kind = kind, Name = obj.Name(), NamespaceProperty = obj.Namespace(), Uid = obj.Uid(),
				ResourceVersion = obj.ResourceVersion()
			};
		}

		/// <summary>Creates a <see cref="V1OwnerReference"/> that refers to the given object.</summary>
		public static V1OwnerReference CreateOwnerReference(this IKubernetesObject<V1ObjectMeta> obj, bool? controller = null, bool? blockDeletion = null)
		{
			if(obj == null) throw new ArgumentNullException(nameof(obj));
			string apiVersion = obj.ApiVersion, kind = obj.Kind; // default to using the API version and kind from the object
			if(string.IsNullOrEmpty(apiVersion) || string.IsNullOrEmpty(kind)) // but if either of them is missing...
			{
				KubernetesScheme.Default.GetVK(obj.GetType(), out apiVersion, out kind); // get it from the default scheme
			}
			return new V1OwnerReference()
			{
				ApiVersion = apiVersion, Kind = kind, Name = obj.Name(), Uid = obj.Uid(), Controller = controller, BlockOwnerDeletion = blockDeletion
			};
		}
	}
}
