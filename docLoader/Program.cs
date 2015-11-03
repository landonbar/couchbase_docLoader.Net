using System;
using Couchbase;
using Couchbase.Configuration.Client;
using System.Collections.Generic;
using System.IO;


namespace docLoader
{
	class MainClass
	{
		public static ClientConfiguration config = new ClientConfiguration
		{
			Servers = new List<Uri>
			{
				new Uri("http://127.0.0.1:8091/pools")
			}
		};

		public static void Main (string[] args)
		{
			Console.WriteLine ("Start Main");

			//TestInsert ();
			LoadDir ();
			WriteFile ();
			//EmptyBucket();

			//Console.Read ();
		}

		public static void TestInsert () {
			using (var cluster = new Cluster(config)) {
				using (var bucket = cluster.OpenBucket ("doc-store")) {
					var document = new Document<dynamic> () {
						Id = "test1",
						Content = new
						{
							name = "Couchbase"
						}
					};

					var upsert = bucket.Upsert (document);
					if (upsert.Success) {
						var get = bucket.GetDocument<dynamic> (document.Id);
						document = get.Document;
						var msg = string.Format ("{0} {1}!", document.Id, document.Content.name);
						Console.WriteLine (msg);
					}

				}
			}
		}
	
		public static void LoadDir(string path="../../TestDocs"){
			Console.WriteLine (path);
			if (Directory.Exists (path)) {
				var fileList = Directory.GetFiles (path);
				foreach (var filePath in fileList) {
					Console.WriteLine (filePath);

					using (var cluster = new Cluster (config)) {
						using (var bucket = cluster.OpenBucket ("doc-store")) {
							LoadFile (filePath, (CouchbaseBucket)bucket);
						}
					}
				}
			}
		}

		public static bool LoadFile(string path, CouchbaseBucket bucket) {
			if (File.Exists (path) ){
				var fi = new FileInfo (path);

				var byteArray = File.ReadAllBytes(path);

				var document = new Document<dynamic> () {
					Id = fi.Name,
					Content = byteArray,
					Expiry = uint.MaxValue  //1.6 months in milliseconds, can be renewed
				};

				var upsert = bucket.Upsert(document);
				if (upsert.Success) {
					return true;
				}
			}
			return false;

		}

		public static bool WriteFile(string name="hello2.txt.zip", string path="../../TestOut")
		{
			using (var cluster = new Cluster(config)) {
				using (var bucket = cluster.OpenBucket ("doc-store")) {
					var fullPath = path + '/' + name;
					var get = bucket.GetDocument<dynamic> (name);
					var document = get.Document;
					var msg = string.Format ("{0}!", document.Id);
					Console.WriteLine (msg);
					var bytes = (byte[]) System.Convert.FromBase64String(document.Content);
					File.WriteAllBytes (fullPath, bytes);
					if (File.Exists (fullPath))
						return true;
					return false;
				}
			}

		}

		//bucket must have flush enabled
		public static void EmptyBucket(string name="doc-store")
		{
			using (var cluster = new Cluster(config)) {
				using (var bucket = cluster.OpenBucket (name)) {
					var manager = bucket.CreateManager("Administrator", "");
					//var result = 
						manager.Flush(); 
				}
			}
		}
	
	}
}
