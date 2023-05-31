using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using sqoDB;

namespace sqoDBDB.Tests
{
    /// <summary>
    ///     Summary description for Join
    /// </summary>
    [TestFixture]
    public class JoinTest34
    {
        private readonly string objPath = TestUtils.GetTempPath();

        public JoinTest34()
        {
            SiaqodbConfigurator.EncryptedDatabase = true;
        }

        /// <summary>
        ///     Gets or sets the test context which provides
        ///     information about and functionality for the current test run.
        /// </summary>
        public TestContext TestContext { get; set; }

        [Test]
        public void SimpleJoin()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DropType<Employee>();
            var s = new List<string>();
            for (var i = 0; i < 20; i++)
            {
                var c = new Customer();
                c.ID = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                ///listInitial.Add(c);
                nop.StoreObject(c);
            }

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();
            var query = (from Customer c in nop
                where c.ID < 3
                join Employee emp in nop
                    on c.ID equals emp.CustomerID
                select new { CName = c.Name, EName = emp.Name }).ToList();

            Assert.AreEqual(3, query.Count);
            //Assert.AreEqual(listInitial[0].Name ,query[0].CName);
            Assert.AreEqual(listInitialEmp[0].Name, query[0].EName);

            var query1 = (from Customer c in nop
                where c.ID < 3
                join Employee emp in nop
                    on c.ID equals emp.CustomerID
                select new { CName = c.Name, EName = emp.Name, EOID = emp.OID, COID = c.OID }).ToList();

            Assert.AreEqual(3, query1.Count);
            //Assert.AreEqual(listInitial[0].Name, query1[0].CName);
            Assert.AreEqual(listInitialEmp[0].Name, query1[0].EName);

            var query2 = (from Customer c in nop
                where c.ID < 3
                join Employee emp in nop
                    on c.OID equals emp.CustomerID
                select new EmpCustOID { CName = c.Name, EName = emp.Name, EOID = emp.OID }).ToList();

            Assert.AreEqual(3, query2.Count);
            //Assert.AreEqual(listInitial[0].Name, query2[0].CName);
            Assert.AreEqual(listInitialEmp[1].OID, query2[0].EOID);
        }

        [Test]
        public void SimpleJoinWithEnum()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<CustomerLite>();
            nop.DropType<EmployeeLite>();
            var s = new List<string>();
            for (var i = 0; i < 10; i++)
            {
                var c = new CustomerLite();

                if (i % 2 == 0)
                    c.TEnum = TestEnum.Unu;
                else
                    c.TEnum = TestEnum.Doi;
                ///listInitial.Add(c);
                nop.StoreObject(c);
            }

            var listInitialEmp = new List<EmployeeLite>();
            for (var j = 0; j < 3; j++)
            {
                var emp = new EmployeeLite();
                emp.EmpEnum = TestEnum.Doi;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();
            var query = (from CustomerLite c in nop
                join EmployeeLite emp in nop
                    on c.TEnum equals emp.EmpEnum
                select new { CName = c.Name, EName = emp.Name }).ToList();

            Assert.AreEqual(15, query.Count);
            Assert.AreEqual(listInitialEmp[1].Name, query[1].EName);
        }

        [Test]
        public void SimpleJoinWithAutomaticProperties()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<CustomerLite>();
            nop.DropType<Employee>();
            var s = new List<string>();
            for (var i = 0; i < 20; i++)
            {
                var c = new CustomerLite();
                c.Age = i;
                if (i % 2 == 0)
                    c.Name = i + "TEST";
                else
                    c.Name = "ADH" + i;
                ///listInitial.Add(c);
                nop.StoreObject(c);
            }

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();
            var query = (from CustomerLite c in nop
                where c.Age < 3
                join Employee emp in nop
                    on c.Age equals emp.CustomerID
                select new { CName = c.Name, EName = emp.Name }).ToList();

            Assert.AreEqual(3, query.Count);
            //Assert.AreEqual(listInitial[0].Name ,query[0].CName);
            Assert.AreEqual(listInitialEmp[0].Name, query[0].EName);

            var query1 = (from CustomerLite c in nop
                where c.Age < 3
                join Employee emp in nop
                    on c.Age equals emp.CustomerID
                select new { CName = c.Name, EName = emp.Name, EOID = emp.OID, COID = c.OID }).ToList();

            Assert.AreEqual(3, query1.Count);
            //Assert.AreEqual(listInitial[0].Name, query1[0].CName);
            Assert.AreEqual(listInitialEmp[0].Name, query1[0].EName);

            var query2 = (from CustomerLite c in nop
                where c.Age < 3
                join Employee emp in nop
                    on c.OID equals emp.CustomerID
                select new EmpCustOID { CName = c.Name, EName = emp.Name, EOID = emp.OID }).ToList();

            Assert.AreEqual(3, query2.Count);
            //Assert.AreEqual(listInitial[0].Name, query2[0].CName);
            Assert.AreEqual(listInitialEmp[1].OID, query2[0].EOID);
        }

        [Test]
        public void SimpleJoinWithWhere()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DropType<Employee>();

            var listInitial = new List<Customer>();
            for (var i = 0; i < 20; i++)
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

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();
            //!!!WARNING not yet optimized
            var query = (from Customer c in nop
                from Employee emp in nop
                where c.ID == emp.CustomerID && c.ID < 3
                select new EmpCust { CName = c.Name, EName = emp.Name }).ToList();

            Assert.AreEqual(3, query.Count);
            Assert.AreEqual(listInitial[0].Name, query[0].CName);
            Assert.AreEqual(listInitialEmp[0].Name, query[0].EName);
        }

        [Test]
        public void CrossJoin()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DropType<Employee>();

            var listInitial = new List<Customer>();
            for (var i = 0; i < 20; i++)
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

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();

            var query = (from Customer c in nop
                from Employee emp in nop
                select new { CName = c.Name, EName = emp.Name }).ToList();

            Assert.AreEqual(200, query.Count);
        }

        [Test]
        public void ComplexJoin()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DropType<Employee>();

            var listInitial = new List<Customer>();
            for (var i = 0; i < 20; i++)
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

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();

            var query = (from Customer p in nop
                let catIds = from Employee c in nop
                    select c.OID
                where catIds.Contains(p.OID) == true
                select new { Product = p.Name, CategoryID = p.OID }).ToList();

            Assert.AreEqual(200, query.Count);
        }

        [Test]
        public void SimpleJoinWithWhereonBothEntities()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DropType<Employee>();

            var listInitial = new List<Customer>();
            for (var i = 0; i < 20; i++)
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

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();

            var query = (from Customer c in nop
                where c.OID > 2
                join Employee emp in nop
                    on c.ID equals emp.CustomerID
                where emp.OID > 4
                select new EmpCust { CName = c.Name, EName = emp.Name }).ToList();

            Assert.AreEqual(6, query.Count);
        }


        [Test]
        public void SimpleJoinUseProperty()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DropType<Employee>();

            var listInitial = new List<Customer>();
            for (var i = 0; i < 20; i++)
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

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();
            var query = (from Customer c in nop
                where c.IDProp < 3
                join Employee emp in nop
                    on c.ID equals emp.CustomerID
                select new EmpCust { CName = c.Name, EName = emp.Name }).ToList();

            Assert.AreEqual(3, query.Count);
            Assert.AreEqual(listInitial[0].Name, query[0].CName);
            Assert.AreEqual(listInitialEmp[0].Name, query[0].EName);

            try
            {
                query = (from Customer c in nop
                    where c.IDPropWithoutAtt < 3
                    join Employee emp in nop
                        on c.ID equals emp.CustomerID
                    select new EmpCust { CName = c.Name, EName = emp.Name }).ToList();


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
                query = (from Customer c in nop
                    where c.IDPropWithNonExistingVar < 3
                    join Employee emp in nop
                        on c.ID equals emp.CustomerID
                    select new EmpCust { CName = c.Name, EName = emp.Name }).ToList();


                foreach (var s in query)
                {
                }
                //Assert.Fail("Property cannot work without Att");
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
        public void SimpleProjectionUsingProperty()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();

            var listInitial = new List<Customer>();
            for (var i = 0; i < 20; i++)
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

            var query = (from Customer c in nop
                select new { CName = c.Name, EName = c.IDProp, c.OID }).ToList();

            for (var i = 0; i < listInitial.Count; i++) Assert.AreEqual(listInitial[i].ID, query[i].EName);
            try
            {
                query = (from Customer c in nop
                    select new { CName = c.Name, EName = c.IDPropWithNonExistingVar, c.OID }).ToList();

                //Assert.Fail("Property cannot work without Att");
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

            try
            {
                query = (from Customer c in nop
                    select new { CName = c.Name, EName = c.IDPropWithoutAtt, c.OID }).ToList();

                //Assert.Fail("Property cannot work without Att");
            }
            catch (Exception ex)
            {
                Assert.AreEqual("A Property must have UseVariable Attribute set", ex.Message);
            }
        }

        [Test]
        public void MultipleJoins()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DropType<Employee>();
            nop.DropType<Order>();

            var listInitial = new List<Customer>();
            for (var i = 0; i < 20; i++)
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

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            var listInitialOrders = new List<Order>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Order();
                emp.EmployeeID = j;
                emp.Name = "Order" + j;
                emp.ID = j;
                listInitialOrders.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();
            var query = (from Customer c in nop
                where c.ID < 3
                join Employee emp in nop
                    on c.ID equals emp.CustomerID
                join Order ord in nop
                    on emp.ID equals ord.EmployeeID
                select new EmpCust { CName = c.Name, EName = emp.Name }).ToList();

            Assert.AreEqual(3, query.Count);
            Assert.AreEqual(listInitial[0].Name, query[0].CName);
            Assert.AreEqual(listInitialEmp[0].Name, query[0].EName);
        }

        [Test]
        public void SimpleJoin2()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DropType<Employee>();

            var listInitial = new List<Customer>();
            for (var i = 0; i < 20; i++)
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

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();
            var query = (from Customer c in nop
                where c.ID < 3
                join Employee emp in nop
                    on c.ID equals emp.CustomerID
                select new { CName = c, EName = emp }).ToList();

            Assert.AreEqual(3, query.Count);
            Assert.AreEqual(listInitial[0].Name, query[0].CName.Name);
            Assert.AreEqual(listInitialEmp[0].Name, query[0].EName.Name);
        }

        [Test]
        public void SimpleJoinOrderBy()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DropType<Employee>();

            var listInitial = new List<Customer>();
            for (var i = 0; i < 20; i++)
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

            var listInitialEmp = new List<Employee>();
            for (var j = 0; j < 10; j++)
            {
                var emp = new Employee();
                emp.CustomerID = j;
                emp.Name = "Employee" + j;
                emp.ID = j;
                listInitialEmp.Add(emp);
                nop.StoreObject(emp);
            }

            nop.Flush();
            var query = from Customer c in nop
                where c.ID < 3
                join Employee emp in nop
                    on c.ID equals emp.CustomerID
                orderby c.Name
                select new { CName = c.Name, EName = emp.Name };
            var k = 0;
            foreach (var s in query)
                if (k == 1)
                    Assert.AreEqual(s.CName, "2TEST");
            query = from Customer c in nop
                where c.ID < 3
                orderby c.Name
                join Employee emp in nop
                    on c.ID equals emp.CustomerID
                select new { CName = c.Name, EName = emp.Name };

            foreach (var s in query)
                if (k == 1)
                    Assert.AreEqual(s.CName, "2TEST");
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