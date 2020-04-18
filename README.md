# KubernetesClient.Fluent
This library extents the [C# Kubernetes client](https://github.com/kubernetes-client/csharp) with a fluent interface, convenient model
extensions, improved watches, and improved exec support.

## Fluent Interface
The library adds a fluent API that is concise, flexible, and works well with custom resources and custom actions. The basic flow is that
from the Kubernetes client, you can create a request, and given a request you can modify or execute it. If `c` is a Kubernetes client,
then these expressions create different types of requests:

    c.Request<V1Pod>() // list all pods in all namespaces
    c.Request<V1Pod>("ns") // list all pods in a namespace
    c.Request<V1Pod>("ns", "name") // get a single pod
    c.Request<V1Pod>("ns", "name").Delete() // delete a pod by name
    c.Request(pod).Post() // create a pod
    c.Request(pod, setBody: false).Delete() // delete a pod if you have the object already
    c.Request<V1Pod>("ns").LabelSelector("X").FieldSelector("Y") // list pods with label and field selectors
    c.Request(pod).Status().Put() // update a pod's status
    c.Request(pod).Subresource("log") // read a pod's logs
    c.Request(pod).Put().DryRun(true) // simulate replacing a resource
    c.Request().RawUri("/apis/…") // specify a raw URL
    etc...

It works with both built-in and custom resource type. For advanced operations, you can:

* Use custom resource types and HTTP verbs
* Modify request and content headers
* Modify the query string
* Access subresources
* Stream the request or upgrade it for web socket or SPDY connections
* Stream the response and access the raw network connection
* Specify group, version, and kind manually, or specify the entire raw URL
* Configure watches

To execute any request so constructed, it's simply: `req.ExecuteAsync()`. This returns a response object that gives you full access to the
status code, response headers, and response stream. (It does not throw an exception if an HTTP error response is returned, unless you want
it to.) It also has convenience methods for reading and deserializing the response in various ways.

A request can be executed multiple times, including in parallel. (I.e. a request is not modified or consumed by executing it.) You can
clone a request if you want to write a method that takes a request and executes a modified version of it without mutating the original.

If you want to execute a request, check the status (including throwing exceptions for errors), and deserialize the body all in one go,
it's simply `req.ExecuteAsync<T>()`.

To assist with modifying resources, it supports an atomic get-modify-update operation via `req.ReplaceAsync<T>(...)`.

## Convenient Model Extensions
The library adds many useful model extension methods that make the Kubernetes client smoother and more concise to use.

* Metadata is inlined, so instead of o.Metadata.X, you can use o.X(). Examples:

      o.Metadata?.Name -> o.Name()
      o.Metadata?.NamespaceProperty -> o.Namespace()
      ... etc ...

* Managing labels and annotations. Examples for labels:

      string value = null;
      if(o.Metadata.Labels != null) o.Metadata.Labels.TryGetValue(key, out value)
  
  becomes
  
      o.GetLabel(key)
  
  Similarly,

      if(o.Metadata == null) o.Metadata = new V1ObjectMeta();
      if(o.Metadata.Labels == null) o.Metadata.Labels = new Dictionary<string,string>();
      o.Metadata.Labels[key] = value;
  
  becomes
  
      o.SetLabel(key, value)

* Managing owner and object references
  * o.AddOwnerReference(ownerRef) // adds an owner reference
  * o.CreateOwnerReference(...) // create an owner reference to o
  * o.FindOwnerReference(owner) // finds and returns the reference to 'owner', if any
  * o.GetController() // gets the reference to the owner that controls the resource
  * o.GetObjectReference() // creates an object reference to o
  * ownerRef.Matches(o) // does ownerRef refer to o?
  * objRef.Matches(o) // does objRef refer to o?
  * ...
* And some other small things.

## Improved Watches
The library adds improved watch support. Aside from various internal problems, the KubernetesClient watch offers only the lowest level
support -- reporting a stream of watch events from a single watch request. In practice, watches time out and need to be reopened, and
when you reopen them you need to resume where you left off, which requires tracking the latest version, but if you're watching a list
of items as opposed to a single item you can't easily do that because the correct version to resume at is not marked in the watch
stream itself and must be retrieved from a list request. Support for resuming and for watching multiple items must be built on top, but
the KubernetesClient Watcher object doesn't have a good interface for building on top of.

The library provides two watch interfaces.

* WatchReader\<T\> -- This class provides the events from a single watch request. It uses a pull-based interface.
      
      // x can be a Stream, HttpResponseMessage, or KubernetesResponse
      using(var wr = new WatchReader<V1Pod>(x))
      {
          WatchEvent<V1Pod> e;
          while((e = await wr.ReadAsync(cancelToken)) != null) /* handle event */
      }
      
* Watch\<T\> -- This class implements a high-level watch that tracks versions, resumes after disconnection, supports watch bookmarks, and
  can watch both lists and single items. It uses an event-based approach like the KubernetesClient Watcher\<T\>, but has more events:
  * Closed
  * EventReceived - Only raised for Added, Modified, and Removed events
  * Error
  * InitialListSent - Raised after the initial list of items was sent from opening the watch. This can be used to let a watch-based cache
    know when it has reached a synchronized state, for example.
  * Opened
  * Reset - Raised when the watch is opened without successfully resuming, i.e. when events may have been lost. This can be used to clear
    caches based on the watch, for example.
  
  ````    
  // request is a KubernetesRequest
  var w = new Watch<V1Pod>(request); // or use request.ToWatch<T>()
  w.EventReceived += ...;
  w.Run();
  …
  w.Dispose(); // or, give it a cancellation token and cancel it
  // if you need to know exactly when it stops you can await the task returned from Run()
  ````

## Improved Exec Support
The library adds improved exec support. The existing exec support uses web sockets, which
[has a flawed protocol](https://github.com/kubernetes/kubernetes/issues/89899) leading to some commands running forever.
You can simply use `request.ExecuteCommandAsync` to execute a command on a resource -- usually but not necessarily a pod.
