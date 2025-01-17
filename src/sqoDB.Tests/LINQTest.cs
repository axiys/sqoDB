﻿using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using sqoDB;

namespace sqoDBDB.Tests
{
    /// <summary>
    ///     Summary description for LINQTest
    /// </summary>
    [TestFixture]
    public class LINQTest
    {
        private readonly string objPath = TestUtils.GetTempPath();

        public LINQTest()
        {
            SiaqodbConfigurator.EncryptedDatabase = true;
        }

        /// <summary>
        ///     Gets or sets the test context which provides
        ///     information about and functionality for the current test run.
        /// </summary>
        public TestContext TestContext { get; set; }

        //TODO: add Enum in where also JOIN etc

        [Test]
        public void TestBasicQuery()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();

            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;

                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                select c;
            Assert.AreEqual(query.ToList().Count, 10);
        }

        [Test]
        public void TestBasicWhere()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.ID < 5
                select c;
            Assert.AreEqual(query.ToList().Count, 5);
            query = from Customer c in nop
                where c.ID > 5
                select c;
            Assert.AreEqual(query.ToList().Count, 4);

            query = from Customer c in nop
                where c.ID == 5
                select c;
            Assert.AreEqual(query.ToList().Count, 1);

            Assert.AreEqual(listInitial[5].Name, query.ToList()[0].Name);
            Assert.AreEqual(listInitial[5].ID, query.ToList()[0].ID);
            Assert.AreEqual(listInitial[5].OID, query.ToList()[0].OID);
        }

        [Test]
        public void TestBasicWhereByOID()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.OID < 5
                select c;
            Assert.AreEqual(query.ToList().Count, 4);
            query = from Customer c in nop
                where c.OID > 5
                select c;
            Assert.AreEqual(query.ToList().Count, 5);

            query = from Customer c in nop
                where c.OID > 5 && c.OID < 8
                select c;
            Assert.AreEqual(query.ToList().Count, 2);


            query = from Customer c in nop
                where c.OID == 5
                select c;
            Assert.AreEqual(query.ToList().Count, 1);

            Assert.AreEqual(listInitial[4].Name, query.ToList()[0].Name);
            Assert.AreEqual(listInitial[4].ID, query.ToList()[0].ID);
            Assert.AreEqual(listInitial[4].OID, query.ToList()[0].OID);
        }

        [Test]
        public void TestBasicWhereOperators()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.ID < 5
                select c;
            Assert.AreEqual(query.ToList().Count, 5);
            query = from Customer c in nop
                where c.ID > 3
                select c;
            Assert.AreEqual(query.ToList().Count, 6);
            query = from Customer c in nop
                where c.ID >= 3
                select c;
            Assert.AreEqual(query.ToList().Count, 7);
            query = from Customer c in nop
                where c.ID <= 3
                select c;
            Assert.AreEqual(query.ToList().Count, 4);

            query = from Customer c in nop
                where c.ID != 3
                select c;
            Assert.AreEqual(query.ToList().Count, 9);
        }

        [Test]
        public void TestBasicWhereStringComparison()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.Name.Contains("ADH")
                select c;

            Assert.AreEqual(query.ToList().Count, 5);
            query = from Customer c in nop
                where c.Name.Contains("2T")
                select c;

            Assert.AreEqual(query.ToList().Count, 1);

            query = from Customer c in nop
                where c.Name.StartsWith("A")
                select c;
            Assert.AreEqual(query.ToList().Count, 5);
            query = from Customer c in nop
                where c.Name.StartsWith("ake")
                select c;


            Assert.AreEqual(query.ToList().Count, 0);
            query = from Customer c in nop
                where c.Name.EndsWith("ADH")
                select c;
            Assert.AreEqual(0, query.ToList().Count);
            query = from Customer c in nop
                where c.Name.EndsWith("TEST")
                select c;
            Assert.AreEqual(5, query.ToList().Count);
        }

        private readonly int id = 3;

        [Test]
        public void WhereLocalVariable()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.ID == id
                select c;

            Assert.AreEqual(query.ToList().Count, 1);
            Assert.AreEqual(3, query.ToList()[0].ID);
        }

        public int TestMet(int t)
        {
            return t + 1;
        }

        public int TestMet2(int t)
        {
            return t + 1;
        }

        public int TestMet3(Customer t)
        {
            return t.ID;
        }

        [Test]
        public void WhereLocalMethod()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.ID == TestMet(3)
                select c;

            Assert.AreEqual(query.ToList().Count, 1);
            Assert.AreEqual(4, query.ToList()[0].ID);

            query = from Customer c in nop
                where c.OID == TestMet(3)
                select c;

            Assert.AreEqual(query.ToList().Count, 1);
            Assert.AreEqual(4, query.ToList()[0].OID);
        }

        [Test]
        public void WhereLocalMethodOverObject()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            //run unoptimized
            var query = from Customer c in nop
                where TestMet2(c.ID) == 3
                select c;

            Assert.AreEqual(query.ToList().Count, 1);
            Assert.AreEqual(2, query.ToList()[0].ID);

            query = from Customer c in nop
                where TestMet3(c) == 3
                select c;

            Assert.AreEqual(query.ToList().Count, 1);
            Assert.AreEqual(3, query.ToList()[0].ID);
        }

        [Test]
        public void WhereAnd()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.Name.Contains("A") && c.Name.Contains("3")
                select c;

            Assert.AreEqual(query.ToList().Count, 1);
            Assert.AreEqual(3, query.ToList()[0].ID);

            query = from Customer c in nop
                where c.Name.Contains("A") && c.Name.Contains("3") && c.ID == 3
                select c;

            Assert.AreEqual(query.ToList().Count, 1);
            Assert.AreEqual(3, query.ToList()[0].ID);
        }

        [Test]
        public void SimpleSelect()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.Name.Contains("A") && c.Name.Contains("3")
                select new { c.Name, Som = c.ID };
            var s = 0;
            foreach (var a in query) s++;
            Assert.AreEqual(1, s);
        }

        [Test]
        public void WhereOR()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.Name.Contains("A") || c.ID == 2
                select c;

            Assert.AreEqual(query.ToList().Count, 6);


            query = from Customer c in nop
                where c.Name.Contains("A") || (c.ID == 2 && c.Name.Contains("T")) || c.ID == 4
                select c;

            Assert.AreEqual(query.ToList().Count, 7);
        }

        [Test]
        public void SelectSimple()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                select new { c.Name, c.ID };

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[k].Name, s.Name);
                Assert.AreEqual(listInitial[k].ID, s.ID);
                k++;
            }

            Assert.AreEqual(k, 10);
        }

        [Test]
        public void SelectSimpleWithDiffType()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                select new { Customerss = c, id = c.ID };

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[k].Name, s.Customerss.Name);
                Assert.AreEqual(listInitial[k].ID, s.id);
                k++;
            }

            Assert.AreEqual(k, 10);
        }

        [Test]
        public void TestUnoptimizedWhere()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.Name.Length == c.ID
                select c;

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[2].Name, s.Name);
                Assert.AreEqual(listInitial[2].ID, s.ID);
            }
            //Assert.AreEqual(k, 1);
        }

        [Test]
        public void TestToString()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.ID.ToString() == "1"
                select c;


            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[1].Name, s.Name);
                Assert.AreEqual(listInitial[1].ID, s.ID);
            }
        }

        [Test]
        public void TestSelfMethod()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.IsTrue(c.Name) == true
                select c;

            Assert.AreEqual(query.ToList().Count, 1);
        }

        [Test]
        public void SelectNonExistingType()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Something>();
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Something c in nop
                select new { c.one, c.two };


            Assert.AreEqual(0, query.ToList().Count);
        }

        [Test]
        public void SelectWhere()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.ID < 3
                select new { c.Name, c.ID };

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[k].Name, s.Name);
                Assert.AreEqual(listInitial[k].ID, s.ID);
                k++;
            }

            Assert.AreEqual(3, k);
        }

        [Test]
        public void SelectWhereUsingProperty()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.IDProp < 3
                select new { c.Name, c.ID };

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[k].Name, s.Name);
                Assert.AreEqual(listInitial[k].ID, s.ID);
                k++;
            }

            Assert.AreEqual(3, k);

            try
            {
                query = from Customer c in nop
                    where c.IDPropWithoutAtt < 3
                    select new { c.Name, c.ID };

                foreach (var s in query)
                {
                }
                //Assert.Fail("Property cannot work without Att");
            }
            catch (Exception ex)
            {
                Assert.AreEqual("A Property must have UseVariable Attribute set", ex.Message);
            }

            try
            {
                query = from Customer c in nop
                    where c.IDPropWithNonExistingVar < 3
                    select new { c.Name, c.ID };

                foreach (var s in query)
                {
                }

                Assert.Fail("Property cannot work without Att");
            }
            catch (Exception ex)
            {
                if (ex.Message.StartsWith("Field:"))
                {
                }
                else
                {
                    Assert.Fail(ex.Message);
                }
            }
        }

        [Test]
        public void SelectWhereUsingAutomaticProperties()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<CustomerLite>();
            var listInitial = new List<CustomerLite>();
            for (var i = 0; i < 10; i++)
            {
                var c = new CustomerLite();
                c.Age = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "Siaqo" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from CustomerLite c in nop
                where c.Age < 3
                select new { c.Name, c.Age };

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[k].Name, s.Name);
                Assert.AreEqual(listInitial[k].Age, s.Age);
                k++;
            }

            Assert.AreEqual(3, k);

            query = from CustomerLite c in nop
                where c.Active == true
                select new { c.Name, c.Age };
            k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[k].Name, s.Name);
                Assert.AreEqual(listInitial[k].Age, s.Age);
                k++;
            }

            Assert.AreEqual(10, k);
        }

        [Test]
        public void SelectWhereUnaryOperator()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<CustomerLite>();
            var listInitial = new List<CustomerLite>();
            for (var i = 0; i < 10; i++)
            {
                var c = new CustomerLite();
                c.Age = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "Siaqo" + i;
                c.Active = false;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();

            //run unoptimized
            var query = (from CustomerLite c in nop
                where c.Age > 5 && !c.Active
                select new { c.Name, c.Age }).ToList();
            var k = 0;

            Assert.AreEqual(4, query.Count);
        }

        [Test]
        public void SelectWhereMinus()
        {
            Assert.Throws<NotSupportedException>(() =>
            {
                var nop = new Siaqodb(objPath);
                nop.DropType<CustomerLite>();
                var listInitial = new List<CustomerLite>();
                for (var i = 0; i < 10; i++)
                {
                    var c = new CustomerLite();
                    c.Age = i;
                    if (i % 2 == 0)
                        c.Name = i + "TEST";
                    else
                        c.Name = "Siaqo" + i;

                    c.Active = false;
                    listInitial.Add(c);
                    nop.StoreObject(c);
                }

                nop.Flush();


                var query = (from CustomerLite c in nop
                    where c.Age + 2 > 0
                    select new { c.Name, c.Age }).ToList();
                var k = 0;

                Assert.AreEqual(3, query.Count);
            });
        }

        [Test]
        public void SelectWhereBooleanAlone()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<CustomerLite>();
            var listInitial = new List<CustomerLite>();
            for (var i = 0; i < 10; i++)
            {
                var c = new CustomerLite();
                c.Age = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "Siaqo" + i;
                //c.Active = true;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();

            //run optimized
            var query = (from CustomerLite c in nop
                where c.Active
                select c).ToList();
            var k = 0;

            Assert.AreEqual(10, query.Count);

            //need some more tests here
            var query1 = (from CustomerLite c in nop
                where c.Age > 5 && c.Active
                select new { c.Name, c.Age }).ToList();


            Assert.AreEqual(4, query1.Count);
        }

        [Test]
        public void OrderByBasic()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 10; i > 0; i--)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.ID > 4
                orderby c.ID
                select new { c.Name, c.ID };

            var k = 0;
            foreach (var s in query)
            {
                if (k == 0) Assert.AreEqual(5, s.ID);
                //Assert.AreEqual(listInitial[k].ID, s.ID);
                k++;
            }
            //Assert.AreEqual(3, k);
        }

        [Test]
        public void SelectWhereUsingEnum()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<CustomerLite>();
            var listInitial = new List<CustomerLite>();
            for (var i = 0; i < 10; i++)
            {
                var c = new CustomerLite();
                c.Name = i.ToString();
                c.Age = i;
                if (i % 3 == 0)
                    c.TEnum = TestEnum.Doi;
                else
                    c.TEnum = TestEnum.Trei;
                listInitial.Add(c);
                nop.StoreObject(c);
            }

            nop.Flush();
            var query = from CustomerLite c in nop
                where c.Age < 3
                select new { c.Name, c.TEnum };

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[k].Name, s.Name);
                Assert.AreEqual(listInitial[k].TEnum, s.TEnum);
                k++;
            }

            Assert.AreEqual(3, k);
        }
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
    }
}