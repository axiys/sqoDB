using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using sqoDB;
using sqoDBDB.Tests;

namespace SiaqodbUnitTests
{
    [TestFixture]
    public class ComplexTypesAsyncTests
    {
        private readonly string dbFolder = TestUtils.GetTempPath();

        public ComplexTypesAsyncTests()
        {
            SiaqodbConfigurator.EncryptedDatabase = false;
        }

        [Test]
        public async Task TestStore()
        {
            var s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            await s_db.DropTypeAsync<A>();
            await s_db.DropTypeAsync<B>();
            await s_db.DropTypeAsync<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                try
                {
                    await s_db.StoreObjectAsync(a);
                }
                catch (Exception ex)
                {
                }
            }

            await s_db.FlushAsync();

            IList<A> allA = await s_db.LoadAllAsync<A>();
            Assert.AreEqual(10, allA.Count);
            Assert.AreEqual(5, allA[5].BVar.Ci.cId);
            Assert.AreEqual(11, allA[2].BVar.bId);
            Assert.AreEqual(5, allA[5].aId);

            IList<B> allB = await s_db.LoadAllAsync<B>();
            Assert.AreEqual(10, allB.Count);
            Assert.AreEqual(5, allB[5].Ci.cId);
            Assert.AreEqual(11, allB[2].bId);

            IList<C> allC = await s_db.LoadAllAsync<C>();
            Assert.AreEqual(10, allC.Count);
            Assert.AreEqual(5, allC[5].cId);
            Assert.AreEqual(11, allC[2].ACircular.BVar.bId);
            Assert.AreEqual(5, allC[5].ACircular.aId);

            allA[0].aId = 100;
            allA[0].BVar.bId = 100;
            allA[0].BVar.Ci.cId = 100;
            await s_db.StoreObjectAsync(allA[0]);
            await s_db.FlushAsync();

            IList<A> allA1 = await s_db.LoadAllAsync<A>();
            Assert.AreEqual(100, allA1[0].aId);
            Assert.AreEqual(100, allA1[0].BVar.bId);
            Assert.AreEqual(100, allA1[0].BVar.Ci.cId);

            allC[1].cId = 200;
            await s_db.StoreObjectAsync(allC[1]);
            await s_db.FlushAsync();
            IList<A> allA2 = await s_db.LoadAllAsync<A>();
            Assert.AreEqual(200, allA2[1].BVar.Ci.cId);
        }

        [Test]
        public async Task TestRead()
        {
            var s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            await s_db.DropTypeAsync<A>();
            await s_db.DropTypeAsync<B>();
            await s_db.DropTypeAsync<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                await s_db.StoreObjectAsync(a);
            }

            await s_db.FlushAsync();
            var q = await (from A a in s_db
                where a.BVar.bId == 11 && a.BVar.Ci.cId == 5
                select a).ToListAsync();

            Assert.AreEqual(1, q.Count);
            Assert.AreEqual(5, q[0].BVar.Ci.cId);

            var q1 = await (from A a in s_db
                where a.aId == 5
                select a.BVar).ToListAsync();
            Assert.AreEqual(1, q1.Count);
            Assert.AreEqual(5, q1[0].Ci.cId);

            var q2 = await (from A a in s_db
                where a.aId == 5
                select new { bVar = a.BVar, cVar = a.BVar.Ci }).ToListAsync();
            Assert.AreEqual(1, q2.Count);
            Assert.AreEqual(5, q2[0].cVar.cId);
        }

        [Test]
        public async Task TestTransaction()
        {
            var s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            await s_db.DropTypeAsync<A>();
            await s_db.DropTypeAsync<B>();
            await s_db.DropTypeAsync<C>();
            var transaction = s_db.BeginTransaction();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                await s_db.StoreObjectAsync(a, transaction);
            }

            await transaction.CommitAsync();

            IList<A> allA = await s_db.LoadAllAsync<A>();
            Assert.AreEqual(10, allA.Count);
            Assert.AreEqual(5, allA[5].BVar.Ci.cId);
            Assert.AreEqual(11, allA[2].BVar.bId);
            Assert.AreEqual(5, allA[5].aId);

            IList<B> allB = await s_db.LoadAllAsync<B>();
            Assert.AreEqual(10, allB.Count);
            Assert.AreEqual(5, allB[5].Ci.cId);
            Assert.AreEqual(11, allB[2].bId);

            IList<C> allC = await s_db.LoadAllAsync<C>();
            Assert.AreEqual(10, allC.Count);
            Assert.AreEqual(5, allC[5].cId);
            Assert.AreEqual(11, allC[2].ACircular.BVar.bId);
            Assert.AreEqual(5, allC[5].ACircular.aId);

            allA[0].aId = 100;
            allA[0].BVar.bId = 100;
            allA[0].BVar.Ci.cId = 100;

            var transaction1 = s_db.BeginTransaction();
            await s_db.StoreObjectAsync(allA[0], transaction1);
            await transaction1.CommitAsync();

            IList<A> allA1 = await s_db.LoadAllAsync<A>();
            Assert.AreEqual(100, allA1[0].aId);
            Assert.AreEqual(100, allA1[0].BVar.bId);
            Assert.AreEqual(100, allA1[0].BVar.Ci.cId);

            allC[1].cId = 200;
            var transaction2 = s_db.BeginTransaction();

            await s_db.StoreObjectAsync(allC[1], transaction2);
            s_db.DeleteAsync(allA1[9], transaction2);
            s_db.DeleteAsync(allA1[8].BVar, transaction2);
            await transaction2.CommitAsync();
            IList<A> allA2 = await s_db.LoadAllAsync<A>();
            IList<B> allB2 = await s_db.LoadAllAsync<B>();
            IList<C> allC2 = await s_db.LoadAllAsync<C>();

            Assert.AreEqual(200, allA2[1].BVar.Ci.cId);
            Assert.AreEqual(9, allA2.Count);
            Assert.AreEqual(9, allB2.Count);
            Assert.AreEqual(10, allC2.Count);
        }

        [Test]
        public async Task TestInclude()
        {
            SiaqodbConfigurator.LoadRelatedObjects<A>(false);
            SiaqodbConfigurator.LoadRelatedObjects<B>(false);
            try
            {
                var s_db = new Siaqodb();
                await s_db.OpenAsync(dbFolder);
                await s_db.DropTypeAsync<A>();
                await s_db.DropTypeAsync<B>();
                await s_db.DropTypeAsync<C>();
                for (var i = 0; i < 10; i++)
                {
                    var a = new A();
                    a.aId = i;
                    a.BVar = new B();
                    a.BVar.bId = 11;
                    a.BVar.Ci = new C();
                    a.BVar.Ci.ACircular = a;
                    a.BVar.Ci.cId = i;
                    await s_db.StoreObjectAsync(a);
                }

                await s_db.FlushAsync();
                IList<A> allA = await s_db.LoadAllAsync<A>();
                IList<B> allB = await s_db.LoadAllAsync<B>();
                for (var i = 0; i < 10; i++)
                {
                    Assert.IsNull(allA[i].BVar);
                    Assert.IsNull(allB[i].Ci);
                }

                var q = await s_db.Cast<A>().Where(a => a.OID > 5).Include("BVar").ToListAsync();

                foreach (var a in q)
                {
                    Assert.IsNotNull(a.BVar);
                    Assert.IsNull(a.BVar.Ci);
                }

                var q1 = await s_db.Cast<A>().Where(a => a.OID > 5).Include("BVar").Include("BVar.Ci").ToListAsync();

                foreach (var a in q1)
                {
                    Assert.IsNotNull(a.BVar);
                    Assert.IsNotNull(a.BVar.Ci);
                }

                var q2 = await s_db.Cast<A>().Where(a => a.OID > 5).Include("BVar").ToListAsync();

                foreach (var a in q2)
                {
                    Assert.IsNotNull(a.BVar);
                    Assert.IsNull(a.BVar.Ci);
                }
            }
            finally
            {
                SiaqodbConfigurator.LoadRelatedObjects<A>(true);
                SiaqodbConfigurator.LoadRelatedObjects<B>(true);
            }
        }

        [Test]
        public async Task TestComplexLists()
        {
            var s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            await s_db.DropTypeAsync<TapRecord>();
            await s_db.DropTypeAsync<D>();

            for (var i = 0; i < 10; i++)
            {
                var d = new D();
                d.tap = new TapRecord();
                d.tap2 = new TapRecord { userName = "newelist" };
                d.TapList = new List<TapRecord>();
                d.TapList.Add(d.tap);
                d.TapList.Add(new TapRecord());
                d.TapList2.Add(new TapRecord { userName = "newelist" });
                await s_db.StoreObjectAsync(d);
            }

            await s_db.FlushAsync();
            IList<D> dlis = await s_db.LoadAllAsync<D>();
            IList<TapRecord> dtap = await s_db.LoadAllAsync<TapRecord>();

            Assert.AreEqual(10, dlis.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(2, dlis[i].TapList.Count);
                Assert.AreEqual(1, dlis[i].TapList2.Count);

                Assert.AreEqual(dlis[i].tap.OID, dlis[i].TapList[0].OID);
                Assert.AreEqual("newelist", dlis[i].TapList2[0].userName);
            }

            var q = await (from D d in s_db
                where d.TapList2.Contains(new TapRecord { userName = "newelist" })
                select d).ToListAsync();
            Assert.AreEqual(10, q.Count);

            var q2 = await (from D d in s_db
                where d.tap == new TapRecord() && d.tap2 == new TapRecord { userName = "newelist" }
                select d).ToListAsync();
            Assert.AreEqual(10, q2.Count);
        }

        [Test]
        public async Task TestWhereComplexObjectCompare()
        {
            var s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            await s_db.DropTypeAsync<TapRecord>();
            await s_db.DropTypeAsync<D>();

            for (var i = 0; i < 10; i++)
            {
                var d = new D();
                d.tap = new TapRecord();
                d.tap2 = new TapRecord { userName = "newelist" };
                d.TapList = new List<TapRecord>();
                d.TapList.Add(d.tap);
                d.TapList.Add(new TapRecord());
                d.TapList2.Add(new TapRecord { userName = "newelist" });
                await s_db.StoreObjectAsync(d);
            }

            await s_db.FlushAsync();
            var q = await (from D d in s_db
                where d.tap2 == new TapRecord { userName = "newelist" }
                select d).ToListAsync();
            Assert.AreEqual(10, q.Count);
        }

        [Test]
        public async Task TestDeleteNestedObject()
        {
            var s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            await s_db.DropTypeAsync<A>();
            await s_db.DropTypeAsync<B>();
            await s_db.DropTypeAsync<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                await s_db.StoreObjectAsync(a);
            }

            await s_db.FlushAsync();
            var q = await (from A a in s_db
                where a.BVar.bId == 11 && a.BVar.Ci.cId == 5
                select a).ToListAsync();

            Assert.AreEqual(1, q.Count);

            await s_db.DeleteAsync(q[0].BVar.Ci);

            await s_db.FlushAsync();
            q = await (from A a in s_db
                where a.BVar.bId == 11 && a.BVar.Ci.cId == 5
                select a).ToListAsync();

            Assert.AreEqual(0, q.Count);
            q = await (from A a in s_db
                where a.BVar.bId == 11
                select a).ToListAsync();

            Assert.AreEqual(10, q.Count);
            Assert.IsNull(q[5].BVar.Ci);

            IList<A> lsA = await s_db.LoadAllAsync<A>();
            await s_db.DeleteAsync(lsA[0].BVar);

            await s_db.FlushAsync();
            IList<C> lsC = await s_db.LoadAllAsync<C>();
            IList<B> lsB = await s_db.LoadAllAsync<B>();
            IList<A> lsA1 = await s_db.LoadAllAsync<A>();

            Assert.AreEqual(9, lsC.Count);
            Assert.AreEqual(9, lsB.Count);
            Assert.AreEqual(10, lsA1.Count);
        }

        [Test]
        public async Task TestListOfLists()
        {
            var s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            await s_db.DropTypeAsync<MyList<int>>();
            for (var i = 0; i < 10; i++)
            {
                var myList = new MyList<int>();
                myList.TheList = new List<ListContainer<int>>();
                var innerList = new ListContainer<int>();
                innerList.List = new List<int>();
                innerList.List.Add(i);
                innerList.List.Add(i + 1);
                myList.TheList.Add(innerList);
                await s_db.StoreObjectAsync(myList);
            }

            await s_db.FlushAsync();
            s_db.Close();
            s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            IList<MyList<int>> list = await s_db.LoadAllAsync<MyList<int>>();
            Assert.AreEqual(10, list.Count);
            Assert.AreEqual(2, list[1].TheList[0].List.Count);
        }

        [Test]
        public async Task TestStorePartialNull()
        {
            var s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            await s_db.DropTypeAsync<A>();
            await s_db.DropTypeAsync<B>();
            await s_db.DropTypeAsync<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                await s_db.StoreObjectAsync(a);
            }

            await s_db.FlushAsync();
            IList<A> lsA = await s_db.LoadAllAsync<A>();
            lsA[0].BVar.Ci = null;
            await s_db.StoreObjectPartiallyAsync(lsA[0].BVar, "Ci");
            await s_db.FlushAsync();
            IList<A> lsA1 = await s_db.LoadAllAsync<A>();
            Assert.IsNull(lsA1[0].BVar.Ci);
        }

        [Test]
        public async Task TestStorePartialOnIndexed()
        {
            SiaqodbConfigurator.AddIndex("cId", typeof(C));

            var s_db = new Siaqodb();
            await s_db.OpenAsync(dbFolder);
            await s_db.DropTypeAsync<A>();
            await s_db.DropTypeAsync<B>();
            await s_db.DropTypeAsync<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i % 2;
                await s_db.StoreObjectAsync(a);
            }

            await s_db.FlushAsync();
            IList<A> lsA = await s_db.LoadAllAsync<A>();
            lsA[0].BVar.Ci.cId = 3;
            await s_db.StoreObjectPartiallyAsync(lsA[0].BVar, "Ci");
            var q = await (from C c in s_db
                where c.cId == 3
                select c).ToListAsync();
            Assert.AreEqual(1, q.Count);
        }
    }

    public class A
    {
        public int aId;
        public int OID { get; set; }
        public B BVar { get; set; }
    }

    public class C
    {
        public A ACircular;
        public int cId;
        public int OID { get; set; }
    }

    public class BB
    {
        public int bId;
        public int OID { get; set; }
        public C Ci { get; set; }
    }

    public class B : BB
    {
        public int bInt;
    }

    public class TapRecord
    {
        public int TotalScore;
        public string userName;
        public int OID { get; set; }

        public async Task AddScore(int ballType)
        {
            TotalScore++;
        }
    }

    public class D
    {
        public TapRecord tap;
        public TapRecord tap2 = new TapRecord { userName = "neww" };
        public List<TapRecord> TapList;
        public List<TapRecord> TapList2 = new List<TapRecord>();
        public int test = 3;
        public int OID { get; set; }
    }

    public class ListContainer<T>
    {
        public List<T> List;
        public int OID { get; set; }
    }

    public class MyList<T>
    {
        public List<ListContainer<T>> TheList;
        public int OID { get; set; }
    }
}