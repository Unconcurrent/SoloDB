using Microsoft.VisualStudio.TestTools.UnitTesting;
using SoloDatabase;
using SoloDatabase.Types;
using System.Text;

namespace CSharpTests;

public class MyType
{
    public SqlId Id { get; set; }
    public string Name { get; set; }
    public string Data { get; set; }
}


[TestClass]
public abstract class ReadMeTests
{
    static byte[] randomBytes = Encoding.UTF8.GetBytes("Hello this is some random data.");

    SoloDB db;

    [TestInitialize]
    public void Init()
    {
        var dbSource = $"memory:Test{Random.Shared.NextInt64()}";
        this.db = new SoloDB(dbSource);
    }

    [TestCleanup]
    public void Cleanup() => this.db.Dispose();

    [TestMethod]
    public void FileStorage()
    {
        var fs = db.FileSystem;

        // Supports directories.
        var directory = fs.GetOrCreateDirAt("/example");

        // Supports directory metadata.
        fs.SetDirectoryMetadata(directory, "key", "value");
        
        fs.Upload("/example/file.txt", new MemoryStream(randomBytes));
        
        // Supports sparse files.
        fs.WriteAt("/example/file.txt", /* offset */ 1000000, randomBytes, /* create if inexistent */true);

        // Supports file metadata.
        fs.SetMetadata("/example/file.txt", "key", "value");


        using var toStream = new MemoryStream();
        fs.Download("/example/file.txt", toStream);
        var read = fs.ReadAt("/example/file.txt", 1000000, randomBytes.Length);

        Assert.IsTrue(read.SequenceEqual(randomBytes));

        var file = fs.GetOrCreateAt("/example/file.txt");

        // Can list files and directories.
        var fileFromListing = fs.ListFilesAt("/example/").First();
        
        // Automatic SHA1 hashing.
        Assert.IsTrue(file.Hash.SequenceEqual(fileFromListing.Hash));

        var fileByHash = fs.GetFileByHash(fileFromListing.Hash);
    }

    [TestMethod]
    public void Example()
    {
        var collection = db.GetCollection<MyType>();

        // Insert a document
        var docId = collection.Insert(new MyType { Id = 0, Name = "Document 1", Data = "Some data" });

        // Or

        var data = new MyType { Id = 0, Name = "Document 1", Data = "Some data" };
        collection.Insert(data);
        Console.WriteLine("{0}", data.Id); // 2

        // Query all documents into a C# list
        var documents = collection.Select().OnAll().Enumerate().ToList();

        // Query the Data property, where Name starts with 'Document'
        var documentsData = collection.Select(d => d.Data).Where(d => d.Name.StartsWith("Document")).Enumerate().ToList();

        data.Data = "Updated data";

        // Update a document
        collection.Update(data);

        // Delete a document
        var count = collection.DeleteById(data.Id);
    }
}
