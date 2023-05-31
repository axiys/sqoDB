using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using sqoDB;
using sqoDBDB.Tests;

namespace SiaqodbUnitTests
{
    [TestFixture]
    public class LINQAsyncTests
    {
        private readonly string dbFolder = TestUtils.GetTempPath();

        [Test]
        public async Task TestBasicQuery()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();

            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;

                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 10);
        }

        [Test]
        public async Task TestBasicWhere()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;
                listInitial.Add(c);
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.ID < 5
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 5);
            query = await (from Customer c in nop
                where c.ID > 5
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 4);

            query = await (from Customer c in nop
                where c.ID == 5
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 1);

            Assert.AreEqual(listInitial[5].Name, query[0].Name);
            Assert.AreEqual(listInitial[5].ID, query[0].ID);
            Assert.AreEqual(listInitial[5].OID, query[0].OID);
        }

        [Test]
        public async Task TestBasicWhereByOID()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;
                listInitial.Add(c);
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.OID < 5
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 4);
            query = await (from Customer c in nop
                where c.OID > 5
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 5);

            query = await (from Customer c in nop
                where c.OID > 5 && c.OID < 8
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 2);


            query = await (from Customer c in nop
                where c.OID == 5
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 1);

            Assert.AreEqual(listInitial[4].Name, query[0].Name);
            Assert.AreEqual(listInitial[4].ID, query[0].ID);
            Assert.AreEqual(listInitial[4].OID, query[0].OID);
        }

        [Test]
        public async Task TestBasicWhereOperators()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
            var listInitial = new List<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;
                listInitial.Add(c);
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.ID < 5
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 5);
            query = await (from Customer c in nop
                where c.ID > 3
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 6);
            query = await (from Customer c in nop
                where c.ID >= 3
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 7);
            query = await (from Customer c in nop
                where c.ID <= 3
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 4);

            query = await (from Customer c in nop
                where c.ID != 3
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 9);
        }

        [Test]
        public async Task TestBasicWhereStringComparison()
        {
            var siaqodb = new Siaqodb();
            await siaqodb.OpenAsync(dbFolder);
            await siaqodb.DropTypeAsync<Customer>();
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
                await siaqodb.StoreObjectAsync(c);
            }

            await siaqodb.FlushAsync();


            var query = await (from Customer c in siaqodb
                where c.Name.Contains("ADH")
                select c).ToListAsync();


            Assert.AreEqual(query.Count, 5);
            query = await (from Customer c in siaqodb
                where c.Name.Contains("2T")
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 1);

            query = await (from Customer c in siaqodb
                where c.Name.StartsWith("A")
                select c).ToListAsync();
            Assert.AreEqual(query.Count, 5);
            query = await (from Customer c in siaqodb
                where c.Name.StartsWith("ake")
                select c).ToListAsync();


            Assert.AreEqual(query.Count, 0);
            query = await (from Customer c in siaqodb
                where c.Name.EndsWith("ADH")
                select c).ToListAsync();
            Assert.AreEqual(0, query.Count);
            query = await (from Customer c in siaqodb
                where c.Name.EndsWith("TEST")
                select c).ToListAsync();
            Assert.AreEqual(5, query.Count);
        }

        private readonly int id = 3;

        [Test]
        public async Task WhereLocalVariable()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.ID == id
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 1);
            Assert.AreEqual(3, query[0].ID);
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
        public async Task WhereLocalMethod()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.ID == TestMet(3)
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 1);
            Assert.AreEqual(4, query[0].ID);

            query = await (from Customer c in nop
                where c.OID == TestMet(3)
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 1);
            Assert.AreEqual(4, query[0].OID);
        }

        [Test]
        public async Task WhereLocalMethodOverObject()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            //run unoptimized
            var query = await (from Customer c in nop
                where TestMet2(c.ID) == 3
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 1);
            Assert.AreEqual(2, query[0].ID);

            query = await (from Customer c in nop
                where TestMet3(c) == 3
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 1);
            Assert.AreEqual(3, query[0].ID);
        }

        [Test]
        public async Task WhereAnd()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.Name.Contains("A") && c.Name.Contains("3")
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 1);
            Assert.AreEqual(3, query[0].ID);

            query = await (from Customer c in nop
                where c.Name.Contains("A") && c.Name.Contains("3") && c.ID == 3
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 1);
            Assert.AreEqual(3, query[0].ID);
        }

        [Test]
        public async Task SimpleSelect()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.Name.Contains("A") && c.Name.Contains("3")
                select new CustomerAnony { Name = c.Name, ID = c.ID }).ToListAsync();
            var s = 0;
            foreach (var a in query) s++;
            Assert.AreEqual(1, s);
        }

        [Test]
        public async Task WhereOR()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.Name.Contains("A") || c.ID == 2
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 6);


            query = await (from Customer c in nop
                where c.Name.Contains("A") || (c.ID == 2 && c.Name.Contains("T")) || c.ID == 4
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 7);
        }

        [Test]
        public async Task SelectSimple()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                select new { c.Name, c.ID }).ToListAsync();

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
        public async Task SelectSimpleWithDiffType()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                select new CustomerAnony { Name = c.Name, ID = c.ID }).ToListAsync();

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
        public async Task TestUnoptimizedWhere()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.Name.Length == c.ID
                select c).ToListAsync();

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[2].Name, s.Name);
                Assert.AreEqual(listInitial[2].ID, s.ID);
            }
            //Assert.AreEqual(k, 1);
        }

        [Test]
        public async Task TestToString()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.ID.ToString() == "1"
                select c).ToListAsync();


            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[1].Name, s.Name);
                Assert.AreEqual(listInitial[1].ID, s.ID);
            }
        }

        [Test]
        public async Task TestSelfMethod()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.IsTrue(c.Name) == true
                select c).ToListAsync();

            Assert.AreEqual(query.Count, 1);
        }

        [Test]
        public async Task SelectNonExistingType()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Something>();
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Something c in nop
                select new SomethingAnony { One = c.one, Two = c.two }).ToListAsync();


            Assert.AreEqual(0, query.ToList().Count);
        }

        [Test]
        public async Task SelectWhere()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.ID < 3
                select new CustomerAnony { Name = c.Name, ID = c.ID }).ToListAsync();

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
        public async Task SelectWhereUsingProperty()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.IDProp < 3
                select c).ToListAsync();

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
                query = await (from Customer c in nop
                    where c.IDPropWithoutAtt < 3
                    select c).ToListAsync();

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
                query = await (from Customer c in nop
                    where c.IDPropWithNonExistingVar < 3
                    select c).ToListAsync();

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
        public async Task SelectWhereUsingAutomaticProperties()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<CustomerLite>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from CustomerLite c in nop
                where c.Age < 3
                select c).ToListAsync();

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[k].Name, s.Name);
                Assert.AreEqual(listInitial[k].Age, s.Age);
                k++;
            }

            Assert.AreEqual(3, k);

            query = await (from CustomerLite c in nop
                where c.Active == true
                select c).ToListAsync();
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
        public async Task SelectWhereUnaryOperator()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<CustomerLite>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();

            //run unoptimized
            var query = await (from CustomerLite c in nop
                where c.Age > 5 && !c.Active
                select new { c.Name, c.Age }).ToListAsync();
            var k = 0;

            Assert.AreEqual(4, query.Count);
        }

        [Test]
        public async Task SelectWhereMinus()
        {
            var exTh = false;
            try
            {
                var nop = new Siaqodb();
                await nop.OpenAsync(dbFolder);
                await nop.DropTypeAsync<CustomerLite>();
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
                    await nop.StoreObjectAsync(c);
                }

                await nop.FlushAsync();


                var query = await (from CustomerLite c in nop
                    where c.Age + 2 > 0
                    select new { c.Name, c.Age }).ToListAsync();
                var k = 0;

                Assert.AreEqual(3, query.Count);
            }
            catch (NotSupportedException ex)
            {
                exTh = true;
            }

            Assert.IsTrue(exTh);
        }

        [Test]
        public async Task SelectWhereBooleanAlone()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<CustomerLite>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();

            //run optimized
            var query = await (from CustomerLite c in nop
                where c.Active
                select c).ToListAsync();
            var k = 0;

            Assert.AreEqual(10, query.Count);

            //need some more tests here
            var query1 = await (from CustomerLite c in nop
                where c.Age > 5 && c.Active
                select c).ToListAsync();


            Assert.AreEqual(4, query1.Count);
        }

        [Test]
        public async Task OrderByBasic()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
            var listInitial = new List<Customer>();
            var j = 0;
            for (var i = 10; i > 0; i--)
            {
                var c = new Customer();
                c.ID = i;

                if (i % 2 == 0)
                    c.Name = "2";
                else
                    c.Name = "3";
                listInitial.Add(c);
                await nop.StoreObjectAsync(c);
                j++;
            }

            await nop.FlushAsync();
            var query = await (from Customer c in nop
                where c.ID > 4
                orderby c.ID
                select c).ToListAsync();

            var k = 0;
            foreach (var s in query)
            {
                if (k == 0) Assert.AreEqual(5, s.ID);
                k++;
            }

            query = await (from Customer c in nop
                where c.ID > 4
                orderby c.ID descending
                select c).ToListAsync();

            k = 0;
            foreach (var s in query)
            {
                if (k == 0) Assert.AreEqual(10, s.ID);
                k++;
            }

            query = await (from Customer c in nop
                where c.ID > 4
                orderby c.ID, c.Name
                select c).ToListAsync();

            k = 0;
            foreach (var s in query)
            {
                if (k == 0) Assert.AreEqual(5, s.ID);
                k++;
            }

            query = await (from Customer c in nop
                where c.ID > 4
                orderby c.Name, c.ID
                select c).ToListAsync();

            k = 0;
            foreach (var s in query)
            {
                if (k == 0) Assert.AreEqual(6, s.ID);
                k++;
            }
            //Assert.AreEqual(3, k);
        }

        [Test]
        public async Task SelectWhereUsingEnum()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<CustomerLite>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (from CustomerLite c in nop
                where c.Age < 3
                select c).ToListAsync();

            var k = 0;
            foreach (var s in query)
            {
                Assert.AreEqual(listInitial[k].Name, s.Name);
                Assert.AreEqual(listInitial[k].TEnum, s.TEnum);
                k++;
            }

            Assert.AreEqual(3, k);
        }

        [Test]
        public async Task SkipTake()
        {
            var nop = new Siaqodb();
            await nop.OpenAsync(dbFolder);
            await nop.DropTypeAsync<Customer>();
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
                await nop.StoreObjectAsync(c);
            }

            await nop.FlushAsync();
            var query = await (await (await (from Customer c in nop
                where c.ID >= 5
                select c).SkipAsync(2)).TakeAsync(2)).ToListAsync();


            Assert.AreEqual(query.Count, 2);
            Assert.AreEqual(query[0].ID, 7);
            Assert.AreEqual(query[1].ID, 8);
        }
    }

    public class CustomerAnony
    {
        public int ID { get; set; }
        public string Name { get; set; }
    }

    public class SomethingAnony
    {
        public int One { get; set; }
        public int Two { get; set; }
    }
}