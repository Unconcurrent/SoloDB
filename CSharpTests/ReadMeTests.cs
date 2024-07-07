using Microsoft.VisualStudio.TestTools.UnitTesting;
using SoloDatabase;
using SoloDatabase.Types;

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
    SoloDB db;

    [TestInitialize]
    public void Init()
    {
        var dbSource = $"memory:Test{Random.Shared.NextInt64()}";
        this.db = SoloDB.Instantiate(dbSource);
    }

    [TestCleanup]
    public void Cleanup() => this.db.Dispose();

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
        var documents = collection.Select().OnAll().ToList();

        // Query the Data property, where Name starts with 'Document'
        var documentsData = collection.Select(d => d.Data).Where(d => d.Name.StartsWith("Document")).ToList();

        data.Data = "Updated data";

        // Update a document
        collection.Update(data);

        // Delete a document
        var count = collection.DeleteById(data.Id);
    }
}
