using System.Dynamic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SoloDatabase;
using static SoloDatabase.Extensions;
using FSharp.Interop.Dynamic;

namespace CSharpTests;

class Money
{
    public string Currency { get; set; }
    public ulong Count { get; set; }
}

class User
{
    public string Name { get; set; }
    public Money[] Money { get; set; }
}


[TestClass]
public abstract class DynamicTests
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
    public void DynamicSelect()
    {
        var users = this.db.GetCollection<User>();
        users.Insert(new User{Name = "Bob1", Money = new []{new Money(){Count = 100, Currency = "MDL"}}});
        users.Insert(new User{Name = "Bob2", Money = new []{new Money(){Count = 100, Currency = "USD"}}});
        users.Insert(new User{Name = "Bob3", Money = new []{new Money(){Count = 100, Currency = "EUR"}}});
        users.Insert(new User{Name = "Bob4", Money = new []{new Money(){Count = 100, Currency = "EUR"}}});

        var allUsersNameWithEUR = 
            users
                .Select(u => u.Name)
                .Where(u => u.Money.AnyInEach(m => m.Currency == "EUR"))
                    .Enumerate()
                .ToList();

        var usersDym = this.db.GetUntypedCollection("User");
        
        var allUsersNameWithEURDyn =
            usersDym
                .Select((u) => u.Dyn("Name"))
                .Where(u => u.Dyn<Money[]>("Money").AnyInEach(m => m.Currency == "EUR"))
                .Enumerate()
                .ToList();

        var allUsersNameWithEURDynTyped =
            usersDym
                .Select((u) => u.Dyn<string>("Name"))
                .Where(u => u.Dyn<Money[]>("Money").AnyInEach(m => m.Currency == "EUR"))
                .Enumerate()
                .ToList();

        Assert.IsTrue(allUsersNameWithEUR.SequenceEqual(allUsersNameWithEURDyn));
        Assert.IsTrue(allUsersNameWithEUR.SequenceEqual(allUsersNameWithEURDynTyped));
    }

    [TestMethod]
    public void DynamicSet()
    {
        var users = this.db.GetCollection<User>();
        var id = users.Insert(new User { Name = "Bob1", Money = new[] { new Money() { Count = 100, Currency = "MDL" } } });
        users.Insert(new User { Name = "Bob2", Money = new[] { new Money() { Count = 100, Currency = "USD" } } });
        users.Insert(new User { Name = "Bob3", Money = new[] { new Money() { Count = 100, Currency = "EUR" } } });
        users.Insert(new User { Name = "Bob4", Money = new[] { new Money() { Count = 100, Currency = "EUR" } } });
        
        var usersDym = this.db.GetUntypedCollection("User");

        usersDym.Update(u => u.Dyn("Name").Set("John"), u => u.Dyn<Money[]>("Money").SetAt(0, new Money() { Count = 100, Currency = "EUR" })).WhereId(id).Execute();
        var updatedUser = (dynamic)usersDym.GetById(id);

        Assert.AreEqual(updatedUser.Name, "John");
        Assert.AreEqual(updatedUser.Money[0].Count, 100);
        Assert.AreEqual(updatedUser.Money[0].Currency, "EUR");
    }
}
