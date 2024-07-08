using System.Dynamic;
using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SoloDatabase;
using static SoloDatabase.Extensions;
using FSharp.Interop.Dynamic;
using SoloDatabase.Types;
using System.Transactions;

namespace CSharpTests;


[TestClass]
public abstract class StandardTests
{
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
    public void DirectSQLOperations()
    {
        using var pooledConnection = this.db.Connection.Borrow();

        long userId;

        using (var transaction = pooledConnection.BeginTransaction())
        {
            try
            {
                pooledConnection.Execute(
                    "CREATE TABLE Users (\r\n    Id INTEGER PRIMARY KEY,\r\n    Name TEXT,\r\n    Age INTEGER\r\n)", transaction);

                // Create a new user
                var insertSql = "INSERT INTO Users (Name, Age) VALUES (@Name, @Age) RETURNING Id;";
                userId = pooledConnection.QuerySingle<long>(insertSql, new { Name = "John Doe", Age = 30 }, transaction);
                Assert.IsTrue(userId > 0, "Failed to insert new user.");
                
                
                // Commit transaction
                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                Assert.Fail($"Test failed with exception: {ex.Message}");
                return;
            }
        }

        // Update the user's age
        var updateSql = "UPDATE Users SET Age = @Age WHERE Id = @Id";
        var affectedRows = pooledConnection.Execute(updateSql, new { Id = userId, Age = 35 });
        Assert.AreEqual(1, affectedRows, "Update affected an unexpected number of rows.");

        // Retrieve the updated user
        var selectSql = "SELECT * FROM Users WHERE Id = @Id";
        var user = pooledConnection.QuerySingleOrDefault<User>(selectSql, new { Id = userId });
        Assert.IsNotNull(user, "User not found.");
        Assert.AreEqual(35, user.Age, "User age not updated correctly.");
    }

    public class User
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
    }
}
