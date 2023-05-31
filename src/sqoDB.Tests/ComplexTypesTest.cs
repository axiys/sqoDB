using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using sqoDB;

namespace sqoDBDB.Tests.M.S
{
    [TestFixture]
    public class ComplexTypesTest
    {
        private readonly string objPath = TestUtils.GetTempPath();

        [Test]
        public void TestStore()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<A>();
            s_db.DropType<B>();
            s_db.DropType<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                s_db.StoreObject(a);
            }

            IList<A> allA = s_db.LoadAll<A>();
            Assert.AreEqual(10, allA.Count);
            Assert.AreEqual(5, allA[5].BVar.Ci.cId);
            Assert.AreEqual(11, allA[2].BVar.bId);
            Assert.AreEqual(5, allA[5].aId);

            IList<B> allB = s_db.LoadAll<B>();
            Assert.AreEqual(10, allB.Count);
            Assert.AreEqual(5, allB[5].Ci.cId);
            Assert.AreEqual(11, allB[2].bId);

            IList<C> allC = s_db.LoadAll<C>();
            Assert.AreEqual(10, allC.Count);
            Assert.AreEqual(5, allC[5].cId);
            Assert.AreEqual(11, allC[2].ACircular.BVar.bId);
            Assert.AreEqual(5, allC[5].ACircular.aId);

            allA[0].aId = 100;
            allA[0].BVar.bId = 100;
            allA[0].BVar.Ci.cId = 100;
            s_db.StoreObject(allA[0]);

            IList<A> allA1 = s_db.LoadAll<A>();
            Assert.AreEqual(100, allA1[0].aId);
            Assert.AreEqual(100, allA1[0].BVar.bId);
            Assert.AreEqual(100, allA1[0].BVar.Ci.cId);

            allC[1].cId = 200;
            s_db.StoreObject(allC[1]);
            IList<A> allA2 = s_db.LoadAll<A>();
            Assert.AreEqual(200, allA2[1].BVar.Ci.cId);
        }

        [Test]
        public void TestRead()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<A>();
            s_db.DropType<B>();
            s_db.DropType<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                s_db.StoreObject(a);
            }

            var q = (from A a in s_db
                where a.BVar.bId == 11 && a.BVar.Ci.cId == 5
                select a).ToList();

            Assert.AreEqual(1, q.Count);
            Assert.AreEqual(5, q[0].BVar.Ci.cId);

            var q1 = (from A a in s_db
                where a.aId == 5
                select a.BVar).ToList();
            Assert.AreEqual(1, q1.Count);
            Assert.AreEqual(5, q1[0].Ci.cId);

            var q2 = (from A a in s_db
                where a.aId == 5
                select new { bVar = a.BVar, cVar = a.BVar.Ci }).ToList();
            Assert.AreEqual(1, q2.Count);
            Assert.AreEqual(5, q2[0].cVar.cId);
        }

        [Test]
        public void TestTransaction()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<A>();
            s_db.DropType<B>();
            s_db.DropType<C>();
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
                s_db.StoreObject(a, transaction);
            }

            transaction.Commit();

            IList<A> allA = s_db.LoadAll<A>();
            Assert.AreEqual(10, allA.Count);
            Assert.AreEqual(5, allA[5].BVar.Ci.cId);
            Assert.AreEqual(11, allA[2].BVar.bId);
            Assert.AreEqual(5, allA[5].aId);

            IList<B> allB = s_db.LoadAll<B>();
            Assert.AreEqual(10, allB.Count);
            Assert.AreEqual(5, allB[5].Ci.cId);
            Assert.AreEqual(11, allB[2].bId);

            IList<C> allC = s_db.LoadAll<C>();
            Assert.AreEqual(10, allC.Count);
            Assert.AreEqual(5, allC[5].cId);
            Assert.AreEqual(11, allC[2].ACircular.BVar.bId);
            Assert.AreEqual(5, allC[5].ACircular.aId);

            allA[0].aId = 100;
            allA[0].BVar.bId = 100;
            allA[0].BVar.Ci.cId = 100;

            var transaction1 = s_db.BeginTransaction();
            s_db.StoreObject(allA[0], transaction1);
            transaction1.Commit();

            IList<A> allA1 = s_db.LoadAll<A>();
            Assert.AreEqual(100, allA1[0].aId);
            Assert.AreEqual(100, allA1[0].BVar.bId);
            Assert.AreEqual(100, allA1[0].BVar.Ci.cId);

            allC[1].cId = 200;
            var transaction2 = s_db.BeginTransaction();

            s_db.StoreObject(allC[1], transaction2);
            s_db.Delete(allA1[9], transaction2);
            s_db.Delete(allA1[8].BVar, transaction2);
            transaction2.Commit();
            IList<A> allA2 = s_db.LoadAll<A>();
            IList<B> allB2 = s_db.LoadAll<B>();
            IList<C> allC2 = s_db.LoadAll<C>();

            Assert.AreEqual(200, allA2[1].BVar.Ci.cId);
            Assert.AreEqual(9, allA2.Count);
            Assert.AreEqual(9, allB2.Count);
            Assert.AreEqual(10, allC2.Count);
        }

        [Test]
        public void TestInclude()
        {
            SiaqodbConfigurator.LoadRelatedObjects<A>(false);
            SiaqodbConfigurator.LoadRelatedObjects<B>(false);
            try
            {
                var s_db = new Siaqodb(objPath);
                s_db.DropType<A>();
                s_db.DropType<B>();
                s_db.DropType<C>();
                for (var i = 0; i < 10; i++)
                {
                    var a = new A();
                    a.aId = i;
                    a.BVar = new B();
                    a.BVar.bId = 11;
                    a.BVar.Ci = new C();
                    a.BVar.Ci.ACircular = a;
                    a.BVar.Ci.cId = i;
                    s_db.StoreObject(a);
                }

                IList<A> allA = s_db.LoadAll<A>();
                IList<B> allB = s_db.LoadAll<B>();
                for (var i = 0; i < 10; i++)
                {
                    Assert.IsNull(allA[i].BVar);
                    Assert.IsNull(allB[i].Ci);
                }

                var q = s_db.Cast<A>().Where(a => a.OID > 5).Include("BVar").ToList();

                foreach (var a in q)
                {
                    Assert.IsNotNull(a.BVar);
                    Assert.IsNull(a.BVar.Ci);
                }

                var q1 = s_db.Cast<A>().Where(a => a.OID > 5).Include("BVar").Include("BVar.Ci").ToList();

                foreach (var a in q1)
                {
                    Assert.IsNotNull(a.BVar);
                    Assert.IsNotNull(a.BVar.Ci);
                }

                var q2 = (from A a in s_db.Query<A>().Include("BVar")
                    select a).ToList();

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
        public void TestComplexLists()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<TapRecord>();
            s_db.DropType<D>();

            for (var i = 0; i < 10; i++)
            {
                var d = new D();
                d.tap = new TapRecord();
                d.tap2 = new TapRecord { userName = "newelist" };
                d.TapList = new List<TapRecord>();
                d.TapList.Add(d.tap);
                d.TapList.Add(new TapRecord());
                d.TapList2.Add(new TapRecord { userName = "newelist" });
                s_db.StoreObject(d);
            }

            IList<D> dlis = s_db.LoadAll<D>();
            IList<TapRecord> dtap = s_db.LoadAll<TapRecord>();

            Assert.AreEqual(10, dlis.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(2, dlis[i].TapList.Count);
                Assert.AreEqual(1, dlis[i].TapList2.Count);

                Assert.AreEqual(dlis[i].tap.OID, dlis[i].TapList[0].OID);
                Assert.AreEqual("newelist", dlis[i].TapList2[0].userName);
            }

            var q = (from D d in s_db
                where d.TapList2.Contains(new TapRecord { userName = "newelist" })
                select d).ToList();
            Assert.AreEqual(10, q.Count);

            var q2 = (from D d in s_db
                where d.tap == new TapRecord() && d.tap2 == new TapRecord { userName = "newelist" }
                select d).ToList();
            Assert.AreEqual(10, q2.Count);
        }

        [Test]
        public void TestWhereComplexObjectCompare()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<TapRecord>();
            s_db.DropType<D>();

            for (var i = 0; i < 10; i++)
            {
                var d = new D();
                d.tap = new TapRecord();
                d.tap2 = new TapRecord { userName = "newelist" };
                d.TapList = new List<TapRecord>();
                d.TapList.Add(d.tap);
                d.TapList.Add(new TapRecord());
                d.TapList2.Add(new TapRecord { userName = "newelist" });
                s_db.StoreObject(d);
            }

            var q = (from D d in s_db
                where d.tap2 == new TapRecord { userName = "newelist" }
                select d).ToList();
            Assert.AreEqual(10, q.Count);
        }

        [Test]
        public void TestDeleteNestedObject()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<A>();
            s_db.DropType<B>();
            s_db.DropType<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                s_db.StoreObject(a);
            }

            var q = (from A a in s_db
                where a.BVar.bId == 11 && a.BVar.Ci.cId == 5
                select a).ToList();

            Assert.AreEqual(1, q.Count);

            s_db.Delete(q[0].BVar.Ci);

            q = (from A a in s_db
                where a.BVar.bId == 11 && a.BVar.Ci.cId == 5
                select a).ToList();

            Assert.AreEqual(0, q.Count);
            q = (from A a in s_db
                where a.BVar.bId == 11
                select a).ToList();

            Assert.AreEqual(10, q.Count);
            Assert.IsNull(q[5].BVar.Ci);

            IList<A> lsA = s_db.LoadAll<A>();
            s_db.Delete(lsA[0].BVar);

            IList<C> lsC = s_db.LoadAll<C>();
            IList<B> lsB = s_db.LoadAll<B>();
            IList<A> lsA1 = s_db.LoadAll<A>();

            Assert.AreEqual(9, lsC.Count);
            Assert.AreEqual(9, lsB.Count);
            Assert.AreEqual(10, lsA1.Count);
        }

        [Test]
        public void TestListOfLists()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<MyList<int>>();
            for (var i = 0; i < 10; i++)
            {
                var myList = new MyList<int>();
                myList.TheList = new List<ListContainer<int>>();
                var innerList = new ListContainer<int>();
                innerList.List = new List<int>();
                innerList.List.Add(i);
                innerList.List.Add(i + 1);
                myList.TheList.Add(innerList);
                s_db.StoreObject(myList);
            }

            s_db.Close();
            s_db = new Siaqodb(objPath);
            IList<MyList<int>> list = s_db.LoadAll<MyList<int>>();
            Assert.AreEqual(10, list.Count);
            Assert.AreEqual(2, list[1].TheList[0].List.Count);
        }

        [Test]
        public void TestStorePartialNull()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<A>();
            s_db.DropType<B>();
            s_db.DropType<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                s_db.StoreObject(a);
            }

            IList<A> lsA = s_db.LoadAll<A>();
            lsA[0].BVar.Ci = null;
            s_db.StoreObjectPartially(lsA[0].BVar, "Ci");
            IList<A> lsA1 = s_db.LoadAll<A>();
            Assert.IsNull(lsA1[0].BVar.Ci);
        }

        [Test]
        public void TestShrinkWithoutComplex()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<AEmpty>();
            var objs = new List<AEmpty>();
            for (var i = 0; i < 10; i++)
            {
                var ae = new AEmpty { MyInt = i };
                ae.SomeStrings = new List<string>();
                ae.SomeStrings.Add("aaa" + i);
                s_db.StoreObject(ae);
                objs.Add(ae);
            }

            for (var i = 0; i < 5; i++) s_db.Delete(objs[i]);

            s_db.Close();
            SiaqodbUtil.Shrink(objPath, ShrinkType.Total);
            s_db = new Siaqodb(objPath);

            IList<AEmpty> all = s_db.LoadAll<AEmpty>();
            for (var i = 0; i < 5; i++) Assert.AreEqual(i + 1, s_db.GetOID(all[i]));
        }

        [Test]
        public void TestShrinkComplex()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<A>();
            s_db.DropType<B>();
            s_db.DropType<C>();
            for (var i = 0; i < 100; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i;
                s_db.StoreObject(a);
            }

            IList<A> all = s_db.LoadAll<A>();
            for (var i = 0; i < 50; i++)
            {
                s_db.Delete(all[i]);
                s_db.Delete(all[i].BVar);
                s_db.Delete(all[i].BVar.Ci);
            }

            s_db.Close();
            var startDate = DateTime.Now;
            SiaqodbUtil.Shrink(objPath, ShrinkType.Total);
            var date = (DateTime.Now - startDate).ToString();

            s_db = new Siaqodb(objPath);
            all = s_db.LoadAll<A>();
            Assert.AreEqual(50, all.Count);
            for (var i = 0; i < 50; i++)
            {
                Assert.AreEqual(i + 1, all[i].OID);
                Assert.AreEqual(i + 1, all[i].BVar.OID);
                Assert.AreEqual(i + 1, all[i].BVar.Ci.OID);
                Assert.AreEqual(i + 50, all[i].aId);
                Assert.AreEqual(11, all[i].BVar.bId);
                Assert.AreEqual(i + 50, all[i].BVar.Ci.cId);
                Assert.AreEqual(all[i].OID, all[i].BVar.Ci.ACircular.OID);
            }
        }

        [Test]
        public void TestStorePartialOnIndexed()
        {
            SiaqodbConfigurator.AddIndex("cId", typeof(C));
            var s_db = new Siaqodb(objPath);
            s_db.DropType<A>();
            s_db.DropType<B>();
            s_db.DropType<C>();
            for (var i = 0; i < 10; i++)
            {
                var a = new A();
                a.aId = i;
                a.BVar = new B();
                a.BVar.bId = 11;
                a.BVar.Ci = new C();
                a.BVar.Ci.ACircular = a;
                a.BVar.Ci.cId = i % 2;
                s_db.StoreObject(a);
            }

            IList<A> lsA = s_db.LoadAll<A>();
            lsA[0].BVar.Ci.cId = 3;
            s_db.StoreObjectPartially(lsA[0].BVar, "Ci");
            var q = (from C c in s_db
                where c.cId == 3
                select c).ToList();
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

        public void AddScore(int ballType)
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

    public class AEmpty
    {
        public int MyInt;
        public List<string> SomeStrings;
    }
}