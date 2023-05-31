using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using NUnit.Framework;
using sqoDB;
using sqoDB.Attributes;
using sqoDB.Exceptions;

namespace sqoDBDB.Tests
{
    /// <summary>
    ///     Summary description for UnitTest1
    /// </summary>
    [TestFixture]
    public class BasicsTest
    {
        private readonly string objPath = TestUtils.GetTempPath();

        public BasicsTest()
        {
            SiaqodbConfigurator.EncryptedDatabase = true;
            // SiaqodbConfigurator.VerboseLevel = VerboseLevel.Info;
            SiaqodbConfigurator.LoggingMethod = LogWarns;
        }

        public void LogWarns(string log, VerboseLevel level)
        {
            Debug.WriteLine(log);
        }

        /// <summary>
        ///     Gets or sets the test context which provides
        ///     information about and functionality for the current test run.
        /// </summary>
        public TestContext TestContext { get; set; }

        [Test]
        public void TestInsert()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();

            for (var i = 10; i < 20; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;
                //c.Vasiel = "momo" + i.ToString();
                nop.StoreObject(c);
            }

            nop.Flush();
            var listC = nop.LoadAll<Customer>();
            Assert.AreEqual(listC.Count, 10);
        }

        [Test]
        public void TestStringWithoutAttribute()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();

            for (var i = 10; i < 20; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;
                c.stringWithoutAtt =
                    "hjqhdlkqwjhedlqkjwhedlkjqhwelkdjhqlwekhdqlkwjehdlkqwjhedlkjqhweljkdhqwlkejdhlqkwjhedlkqjwhedlkjqhwekldjhqlkwejdhlqkjwehdlkqjwhedlkjhwedkljqhweldkjhqwelkhdqlwkjehdlqkjwhedlkjqwhedlkjhqweljdhqwlekjdhlqkwjehdlkjqwhedlkjwq________________________********************************************************************";
                nop.StoreObject(c);
            }

            nop.Flush();
            var listC = nop.LoadAll<Customer>();

            Assert.AreEqual(100, listC[0].stringWithoutAtt.Length);
        }

        [Test]
        public void TestSchemaChanged()
        {
            Assert.Throws<TypeChangedException>(() =>
            {
                var nop = new Siaqodb(objPath);
                //nop.DropType<Something32>();

                for (var i = 10; i < 20; i++)
                {
                    var c = new Something32();
                    c.one = i;
                    c.three = i;
                    //c.two = i;

                    nop.StoreObject(c);
                }

                nop.Flush();
                var listC = nop.LoadAll<Something32>();
                Assert.AreEqual(listC.Count, 10);
            });
        }

        [Test]
        public void TestMassInsert()
        {
            var nop = new Siaqodb(objPath);
            var start = DateTime.Now;
            for (var i = 0; i < 100; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;

                nop.StoreObject(c);
            }

            nop.Flush();
            var t = (DateTime.Now - start).ToString();
            Console.WriteLine(t);
        }

        [Test]
        public void TestInsertAllTypeOfFields()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<D40>();
            var d = new D40();
            d.b = 10;

            d.bo = true;
            d.c = 'c';
            d.d = 10;
            d.de = 10;
            d.dt = DateTime.Now;
            d.dtsofs = DateTime.Now;
            d.f = 10;
            d.g = Guid.NewGuid();
            d.ID = 10;
            d.iu = 10;
            d.l = 10;
            d.s = 1;
            d.sb = 1;
            d.ts = new TimeSpan();
            d.ul = 10;
            d.us = 1;
            d.enn = myEnum.unu;
            d.str = "Abramé";
            d.Text = "this is a text with unlimitted nr of chars! ";
            var g = d.g;
            var ts = d.ts;
            var dt = d.dt;
            var dtsofs = d.dtsofs;
            nop.StoreObject(d);

            var all1 = nop.LoadAll<D40>();
            foreach (var dL in all1)
            {
                Assert.AreEqual(d.b, dL.b);
                Assert.AreEqual(d.bo, dL.bo);
                Assert.AreEqual(d.c, dL.c);
                Assert.AreEqual(d.d, dL.d);
                Assert.AreEqual(d.de, dL.de);
                Assert.AreEqual(DateTime.Now.Month, dL.dt.Month);
                Assert.AreEqual(DateTime.Now.Day, dL.dt.Day);
                Assert.AreEqual(DateTime.Now.Year, dL.dt.Year);
                Assert.AreEqual(dt, dL.dt);
                Assert.AreEqual(dtsofs, dL.dtsofs);
                Assert.AreEqual(dtsofs.Offset, dL.dtsofs.Offset);
                Assert.AreEqual(d.f, dL.f);
                Assert.AreEqual(g, dL.g);
                Assert.AreEqual(d.ID, dL.ID);
                Assert.AreEqual(d.iu, dL.iu);
                Assert.AreEqual(d.l, dL.l);
                Assert.AreEqual(d.s, dL.s);
                Assert.AreEqual(d.sb, dL.sb);
                Assert.AreEqual(ts, dL.ts);
                Assert.AreEqual(d.ul, dL.ul);
                Assert.AreEqual(d.us, dL.us);
                Assert.AreEqual(myEnum.unu, dL.enn);
                Assert.AreEqual("Abramé", dL.str);
                Assert.AreEqual(d.Text, dL.Text);
            }

            nop.Close();
            nop = new Siaqodb(objPath);
            var all = nop.LoadAll<D40>();
            foreach (var dL in all)
            {
                Assert.AreEqual(d.b, dL.b);
                Assert.AreEqual(d.bo, dL.bo);
                Assert.AreEqual(d.c, dL.c);
                Assert.AreEqual(d.d, dL.d);
                Assert.AreEqual(d.de, dL.de);
                Assert.AreEqual(DateTime.Now.Month, dL.dt.Month);
                Assert.AreEqual(DateTime.Now.Day, dL.dt.Day);
                Assert.AreEqual(DateTime.Now.Year, dL.dt.Year);
                Assert.AreEqual(dt, dL.dt);
                Assert.AreEqual(dtsofs, dL.dtsofs);
                Assert.AreEqual(dtsofs.Offset, dL.dtsofs.Offset);

                Assert.AreEqual(d.f, dL.f);
                Assert.AreEqual(g, dL.g);
                Assert.AreEqual(d.ID, dL.ID);
                Assert.AreEqual(d.iu, dL.iu);
                Assert.AreEqual(d.l, dL.l);
                Assert.AreEqual(d.s, dL.s);
                Assert.AreEqual(d.sb, dL.sb);
                Assert.AreEqual(ts, dL.ts);
                Assert.AreEqual(d.ul, dL.ul);
                Assert.AreEqual(d.us, dL.us);
                Assert.AreEqual(myEnum.unu, dL.enn);
                Assert.AreEqual("Abramé", dL.str);
                Assert.AreEqual(d.Text, dL.Text);
            }
        }

        [Test]
        public void TestUpdate()
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
            var listC = nop.LoadAll<Customer>();
            Assert.AreEqual(listC.Count, 10);
            listC[0].Name = "UPDATEWORK";

            nop.StoreObject(listC[0]);
            nop.Close();
            nop = new Siaqodb(objPath);
            var listCUpdate = nop.LoadAll<Customer>();
            Assert.AreEqual("UPDATEWORK", listCUpdate[0].Name);
        }

        [Test]
        public void TestUpdateCheckNrRecords()
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
            var listC = nop.LoadAll<Customer>();
            Assert.AreEqual(listC.Count, 10);
            listC[0].Name = "UPDATEWORK";

            nop.StoreObject(listC[0]);
            nop.Close();
            nop = new Siaqodb(objPath);
            var listCUpdate = nop.LoadAll<Customer>();
            Assert.AreEqual("UPDATEWORK", listCUpdate[0].Name);
            Assert.AreEqual(10, listCUpdate.Count);
        }

        [Test]
        public void TestInsertAfterDrop()
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
            var listC = nop.LoadAll<Customer>();
            nop.DropType<Customer>();
            nop.StoreObject(listC[0]);

            nop.Close();
            nop = new Siaqodb(objPath);
            var listCUpdate = nop.LoadAll<Customer>();
            Assert.AreEqual(1, listCUpdate.Count);
        }

        [Test]
        public void TestSavingEvent()
        {
            nrSaves = 0;

            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.SavingObject += nop_SavingObject;
            nop.SavedObject += nop_SavedObject;
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;

                nop.StoreObject(c);
            }

            nop.Flush();

            var listC = nop.LoadAll<Customer>();

            Assert.AreEqual(0, listC.Count);
            Assert.AreEqual(0, nrSaves);
        }

        [Test]
        public void TestSavedEvent()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.SavedObject += nop_SavedObject;
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;

                nop.StoreObject(c);
            }

            nop.Flush();

            var listC = nop.LoadAll<Customer>();

            Assert.AreEqual(10, listC.Count);
            Assert.AreEqual(10, nrSaves);
        }

        [Test]
        public void TestDelete()
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

            var listC = nop.LoadAll<Customer>();

            nop.Delete(listC[0]);
            nop.Delete(listC[1]);

            var listDeleted = nop.LoadAll<Customer>();

            Assert.AreEqual(8, listDeleted.Count);
            Assert.AreEqual(3, listDeleted[0].OID);
        }

        [Test]
        public void TestDeleteEvents()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            nop.DeletingObject += nop_DeletingObject;
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;

                nop.StoreObject(c);
            }

            nop.Flush();

            var listC = nop.LoadAll<Customer>();

            nop.Delete(listC[0]);
            nop.Delete(listC[1]);

            var listDeleted = nop.LoadAll<Customer>();

            Assert.AreEqual(10, listDeleted.Count);
            Assert.AreEqual(1, listDeleted[0].OID);
        }
        //removed for safety reason
        //[Test]
        //public void TestDeleteByOID()
        //{
        //    Siaqodb nop = new Siaqodb(objPath);
        //    nop.DropType<Customer>();

        //    for (int i = 0; i < 10; i++)
        //    {
        //        Customer c = new Customer();
        //        c.ID = i;
        //        c.Name = "ADH" + i.ToString();

        //        nop.StoreObject(c);
        //    }
        //    nop.Flush();

        //    IObjectList<Customer> listC = nop.LoadAll<Customer>();

        //    nop.DeleteByOID<Customer>(listC[0].OID);
        //    nop.DeleteByOID <Customer>(listC[1].OID);

        //    IObjectList<Customer> listDeleted = nop.LoadAll<Customer>();

        //    Assert.AreEqual(8, listDeleted.Count);
        //    Assert.AreEqual(3, listDeleted[0].OID);

        //}

        private void nop_DeletingObject(object sender, DeletingEventsArgs e)
        {
            e.Cancel = true;
        }

        private int nrSaves;

        private void nop_SavedObject(object sender, SavedEventsArgs e)
        {
            nrSaves++;
        }

        private void nop_SavingObject(object sender, SavingEventsArgs e)
        {
            e.Cancel = true;
        }

        [Test]
        public void TestCount()
        {
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();
            for (var i = 0; i < 160; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "ADH" + i;

                nop.StoreObject(c);
            }

            nop.Flush();

            var listC = nop.LoadAll<Customer>();
            nop.Delete(listC[0]);
            var count = nop.Count<Customer>();
            Assert.AreEqual(160, listC.Count);
            Assert.AreEqual(159, count);
        }

        [Test]
        public void TestSaveDeletedObject()
        {
            Assert.Throws<SiaqodbException>(() =>
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

                var listC = nop.LoadAll<Customer>();
                nop.Delete(listC[0]);

                nop.StoreObject(listC[0]);
            });
        }

        [Test]
        public void TestDeleteUnSavedObject()
        {
            Assert.Throws<SiaqodbException>(() =>
            {
                var nop = new Siaqodb(objPath);
                nop.DropType<Customer>();

                var cu = new Customer();
                cu.ID = 78;
                nop.Delete(cu);
            });
        }

        [Test]
        public void TestXMLExportImport()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<Customer>();
            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = null;

                sq.StoreObject(c);
            }

            sq.Flush();
            var cust = sq.LoadAll<Customer>();
            var sb = new StringBuilder();
            var xmlSer = XmlWriter.Create(sb);
            sq.ExportToXML<Customer>(xmlSer);
            xmlSer.Close();

            var xmlSerRea = XmlReader.Create(new StringReader(sb.ToString()));
            var l = sq.ImportFromXML<Customer>(xmlSerRea);

            xmlSerRea.Close();
            Assert.AreEqual(10, l.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(cust[i].ID, l[i].ID);
                Assert.AreEqual(cust[i].Name, l[i].Name);
            }
        }

        [Test]
        public void TestXMLExportImportCompleteType()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<D40>();
            for (var i = 0; i < 10; i++)
            {
                var d = new D40();
                d.b = 10;

                d.bo = true;
                d.c = 'c';
                d.d = 10;
                d.de = 10;
                d.dt = DateTime.Now;
                d.f = 10;
                d.g = Guid.NewGuid();
                d.ID = 10;
                d.iu = 10;
                d.l = 10;
                d.s = 1;
                d.sb = 1;
                d.ts = new TimeSpan();
                d.ul = 10;
                d.us = 1;
                d.enn = myEnum.doi;
                d.str = "Abramé";

                var g = d.g;
                var ts = d.ts;
                var dt = d.dt;

                sq.StoreObject(d);
            }

            sq.Flush();
            var cust = sq.LoadAll<D40>();
            var sb = new StringBuilder();
            var xmlSer = XmlWriter.Create(sb);
            sq.ExportToXML<D40>(xmlSer);
            xmlSer.Close();

            var xmlSerRea = XmlReader.Create(new StringReader(sb.ToString()));
            var l = sq.ImportFromXML<D40>(xmlSerRea);

            xmlSerRea.Close();
            Assert.AreEqual(10, l.Count);
            for (var i = 0; i < 10; i++)
            {
                var d = cust[i];
                var dL = l[i];
                Assert.AreEqual(d.b, dL.b);
                Assert.AreEqual(d.bo, dL.bo);
                Assert.AreEqual(d.c, dL.c);
                Assert.AreEqual(d.d, dL.d);
                Assert.AreEqual(d.de, dL.de);
                Assert.AreEqual(DateTime.Now.Month, dL.dt.Month);
                Assert.AreEqual(DateTime.Now.Day, dL.dt.Day);
                Assert.AreEqual(DateTime.Now.Year, dL.dt.Year);
                Assert.AreEqual(d.dt, dL.dt);
                Assert.AreEqual(d.f, dL.f);
                Assert.AreEqual(d.g, dL.g);
                Assert.AreEqual(d.ID, dL.ID);
                Assert.AreEqual(d.iu, dL.iu);
                Assert.AreEqual(d.l, dL.l);
                Assert.AreEqual(d.s, dL.s);
                Assert.AreEqual(d.sb, dL.sb);
                Assert.AreEqual(d.ts, dL.ts);
                Assert.AreEqual(d.ul, dL.ul);
                Assert.AreEqual(d.us, dL.us);
                Assert.AreEqual(myEnum.doi, dL.enn);
                Assert.AreEqual("Abramé", dL.str);
            }
        }

        [Test]
        public void TestXMLExportImportCompleteTypeNullable()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<D40Nullable>();
            for (var i = 0; i < 10; i++)
            {
                var d = new D40Nullable();
                d.b = 10;


                sq.StoreObject(d);
            }

            sq.Flush();
            var cust = sq.LoadAll<D40Nullable>();
            var sb = new StringBuilder();
            var xmlSer = XmlWriter.Create(sb);
            sq.ExportToXML<D40Nullable>(xmlSer);
            xmlSer.Close();

            var xmlSerRea = XmlReader.Create(new StringReader(sb.ToString()));
            var l = sq.ImportFromXML<D40Nullable>(xmlSerRea);

            xmlSerRea.Close();
            Assert.AreEqual(10, l.Count);
        }

        [Test]
        public void TestUniqueExceptionInsert()
        {
            Assert.Throws<UniqueConstraintException>(() =>
            {
                var sq = new Siaqodb(objPath);
                sq.DropType<ItemUnique>();

                var c = new ItemUnique();
                c.Age = 10;
                c.S = "ceva";

                sq.StoreObject(c);
                c.S = "cevaa";
                sq.StoreObject(c);
                sq.Flush();

                var c1 = new ItemUnique();
                c1.Age = 11;
                c1.S = "cevaa";

                sq.StoreObject(c1);
            });
        }

        [Test]
        public void TestUniqueExceptionInsertTransaction()
        {
            Assert.Throws<UniqueConstraintException>(() =>
            {
                var sq = new Siaqodb(objPath);
                sq.DropType<ItemUnique>();

                var c = new ItemUnique();
                c.Age = 10;
                c.S = "ceva";

                sq.StoreObject(c);
                c.S = "cevaa";
                sq.StoreObject(c);
                sq.Flush();

                var c1 = new ItemUnique();
                c1.Age = 11;
                c1.S = "cevaa";

                var tr = sq.BeginTransaction();
                sq.StoreObject(c1, tr);
                tr.Commit();
            });
        }

        [Test]
        public void TestUniqueExceptionUpdate()
        {
            Assert.Throws<UniqueConstraintException>(() =>
            {
                var sq = new Siaqodb(objPath);
                sq.DropType<ItemUnique>();

                var c = new ItemUnique();
                c.Age = 10;
                c.S = "ceva";

                sq.StoreObject(c);
                c.S = "ceva";
                sq.StoreObject(c);
                sq.Flush();

                var c1 = new ItemUnique();
                c1.Age = 11;
                c1.S = "ceva1";

                sq.StoreObject(c1);

                var list = sq.LoadAll<ItemUnique>();
                list[1].S = "ceva";
                sq.StoreObject(list[1]); //should throw exception
            });
        }

        [Test]
        public void TestUpdateObjectBy()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ItemUnique>();

            var c = new ItemUnique();
            c.Age = 10;
            c.S = "some";
            sq.StoreObject(c);

            var c1 = new ItemUnique();
            c1.Age = 11;
            c1.S = "some1";

            sq.StoreObject(c1);

            var it = new ItemUnique();
            it.Age = 11;
            it.S = "someNew";
            var stored = sq.UpdateObjectBy("Age", it);
            Assert.IsTrue(stored);

            var list = sq.LoadAll<ItemUnique>();

            Assert.AreEqual("someNew", list[1].S);


            it = new ItemUnique();
            it.Age = 13;
            it.S = "someNew";
            stored = sq.UpdateObjectBy("Age", it);
            Assert.IsFalse(stored);
        }

        [Test]
        public void TestUpdateObjectByDuplicates()
        {
            Assert.Throws<TypeChangedException>(() =>
            {
                var sq = new Siaqodb(objPath);
                sq.DropType<Employee>();

                var emp = new Employee();
                emp.ID = 100;

                sq.StoreObject(emp);

                emp = new Employee();
                emp.ID = 100;
                sq.StoreObject(emp);

                emp = new Employee();
                emp.ID = 100;

                sq.UpdateObjectBy("ID", emp);
            });
        }

        [Test]
        public void TestUpdateObjectByFieldNotExists()
        {
            Assert.Throws<TypeChangedException>(() =>
            {
                var sq = new Siaqodb(objPath);
                sq.DropType<Employee>();

                var emp = new Employee();
                emp.ID = 100;

                sq.StoreObject(emp);

                sq.UpdateObjectBy("IDhh", emp);
            });
        }

        [Test]
        public void TestUpdateObjectByManyFieldsDuplicates()
        {
            Assert.Throws<TypeChangedException>(() =>
            {
                var sq = new Siaqodb(objPath);
                sq.DropType<Employee>();

                var emp = new Employee();
                emp.ID = 100;
                emp.CustomerID = 30;

                sq.StoreObject(emp);

                emp = new Employee();
                emp.ID = 100;
                emp.CustomerID = 30;

                sq.StoreObject(emp);

                emp = new Employee();
                emp.ID = 100;
                emp.CustomerID = 30;

                sq.UpdateObjectBy(emp, "ID", "CustomerID");
            });
        }

        [Test]
        public void TestUpdateObjectByManyFields()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<Employee>();

            var emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "s";
            sq.StoreObject(emp);


            emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "s";

            var s = sq.UpdateObjectBy(emp, "ID", "CustomerID", "Name");

            Assert.IsTrue(s);
        }

        [Test]
        public void TestDeleteObjectBy()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<Employee>();

            var emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "s";
            sq.StoreObject(emp);


            emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "s";

            var s = sq.DeleteObjectBy(emp, "ID", "CustomerID", "Name");

            Assert.IsTrue(s);

            emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "s";
            sq.StoreObject(emp);

            emp = new Employee();
            emp.ID = 100;

            s = sq.DeleteObjectBy("ID", emp);

            Assert.IsTrue(s);
        }

        [Test]
        public void TestUpdateObjectByManyFieldsConstraints()
        {
            Assert.Throws<UniqueConstraintException>(() =>
            {
                var sq = new Siaqodb(objPath);
                sq.DropType<ItemUnique>();

                var emp = new ItemUnique();
                emp.Age = 100;
                emp.integ = 10;
                emp.S = "g";
                sq.StoreObject(emp);

                emp = new ItemUnique();
                emp.Age = 110;
                emp.integ = 10;
                emp.S = "gg";
                sq.StoreObject(emp);

                emp = new ItemUnique();
                emp.Age = 100;
                emp.integ = 10;
                emp.S = "gge";


                var s = sq.UpdateObjectBy(emp, "Age", "integ");
                Assert.IsTrue(s);

                emp = new ItemUnique();
                emp.Age = 100;
                emp.integ = 10;
                emp.S = "gg";

                s = sq.UpdateObjectBy(emp, "Age", "integ");
            });
        }

        [Test]
        public void TestEventsVariable()
        {
            var sq = new Siaqodb(objPath);
            //sq.DropType<ClassWithEvents>();

            var c = new ClassWithEvents();
            c.one = 10;


            sq.StoreObject(c);
            var ll = sq.LoadAll<ClassWithEvents>();
        }

        [Test]
        public void TestIndexFirstInsert()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ClassIndexes>();
            for (var i = 0; i < 100; i++)
            {
                var cls = new ClassIndexes();
                cls.one = i % 10;
                cls.two = i % 10 + 1;
                sq.StoreObject(cls);
            }

            var q = from ClassIndexes clss in sq
                where clss.one == 9
                select clss;


            Assert.AreEqual(10, q.Count());

            sq = new Siaqodb(objPath);
            q = from ClassIndexes clss in sq
                where clss.two == 10
                select clss;


            Assert.AreEqual(10, q.Count());
        }

        [Test]
        public void TestIndexUpdate()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ClassIndexes>();
            for (var i = 0; i < 100; i++)
            {
                var cls = new ClassIndexes();
                cls.one = i % 10;
                cls.two = i % 10 + 1;
                sq.StoreObject(cls);
            }

            sq = new Siaqodb(objPath);
            var q = from ClassIndexes clss in sq
                where clss.one == 9
                select clss;


            q.ToList()[0].one = 5;

            sq.StoreObject(q.ToList()[0]);

            sq.StoreObject(q.ToList()[1]); //just update nothing change

            sq = new Siaqodb(objPath);
            q = from ClassIndexes clss in sq
                where clss.one == 9
                select clss;


            Assert.AreEqual(9, q.Count());

            q = from ClassIndexes clss in sq
                where clss.one == 5
                select clss;


            Assert.AreEqual(11, q.Count());
        }

        [Test]
        public void TestIndexSaveAndClose()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ClassIndexes>();
            for (var i = 0; i < 100; i++)
            {
                var cls = new ClassIndexes();
                cls.one = i % 10;
                cls.two = i % 10 + 1;
                sq.StoreObject(cls);
            }

            sq = new Siaqodb(objPath);
            var q = from ClassIndexes clss in sq
                where clss.one == 9
                select clss;


            Assert.AreEqual(10, q.Count());
        }

        [Test]
        public void TestIndexAllOperations()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ClassIndexes>();
            for (var i = 0; i < 100; i++)
            {
                var cls = new ClassIndexes();
                cls.one = i % 10;
                cls.two = i % 10 + 1;
                sq.StoreObject(cls);
            }

            sq.Close();
            sq = new Siaqodb(objPath);
            var q = from ClassIndexes clss in sq
                where clss.one <= 2
                select clss;


            Assert.AreEqual(30, q.Count());

            q = from ClassIndexes clss in sq
                where clss.one < 2
                select clss;


            Assert.AreEqual(20, q.Count());
            q = from ClassIndexes clss in sq
                where clss.one >= 2
                select clss;


            Assert.AreEqual(80, q.Count());
            q = from ClassIndexes clss in sq
                where clss.one > 2
                select clss;


            Assert.AreEqual(70, q.Count());
        }

        [Test]
        public void TestIndexUpdateObjectBy()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ClassIndexes>();
            for (var i = 0; i < 100; i++)
            {
                var cls = new ClassIndexes();
                cls.one = i % 10;
                cls.two = i % 10 + 1;
                cls.ID = i;
                cls.ID2 = i;
                sq.StoreObject(cls);
            }

            sq = new Siaqodb(objPath);
            var q = from ClassIndexes clss in sq
                where clss.two == 4
                select clss;

            q.ToList()[0].two = 5;
            sq.UpdateObjectBy("ID", q.ToList()[0]);

            q = from ClassIndexes clss in sq
                where clss.two == 4
                select clss;

            Assert.AreEqual(9, q.Count());

            q = from ClassIndexes clss in sq
                where clss.two == 5
                select clss;
            Assert.AreEqual(11, q.Count());

            q.ToList()[0].two = 6;
            sq.UpdateObjectBy("ID2", q.ToList()[0]);

            q = from ClassIndexes clss in sq
                where clss.two == 5
                select clss;
            Assert.AreEqual(10, q.Count());

            q = from ClassIndexes clss in sq
                where clss.two == 6
                select clss;
            Assert.AreEqual(11, q.Count());
        }

        [Test]
        public void TestIndexDelete()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ClassIndexes>();
            for (var i = 0; i < 100; i++)
            {
                var cls = new ClassIndexes();
                cls.one = i % 10;
                cls.two = i % 10 + 1;
                cls.ID = i;
                cls.ID2 = i;
                sq.StoreObject(cls);
            }

            sq = new Siaqodb(objPath);

            var q = from ClassIndexes clss in sq
                where clss.two == 7
                select clss;


            sq.Delete(q.ToList()[0]);
            sq = new Siaqodb(objPath);
            q = from ClassIndexes clss in sq
                where clss.two == 7
                select clss;

            Assert.AreEqual(9, q.Count());

            sq.DeleteObjectBy("ID", q.ToList()[0]);

            sq = new Siaqodb(objPath);
            q = from ClassIndexes clss in sq
                where clss.two == 7
                select clss;

            Assert.AreEqual(8, q.Count());


            sq.DeleteObjectBy("ID2", q.ToList()[0]);

            q = from ClassIndexes clss in sq
                where clss.two == 7
                select clss;

            Assert.AreEqual(7, q.Count());
        }

        [Test]
        public void TestIndexAllFieldTypes()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<D40WithIndexes>();

            var dt = new DateTime(2010, 1, 1);
            var guid = Guid.NewGuid();
            var tspan = new TimeSpan();
            for (var i = 0; i < 10; i++)
            {
                var d = new D40WithIndexes();
                d.b = Convert.ToByte(i);

                d.bo = true;
                d.c = 'c';
                d.d = i;
                d.de = i;
                d.dt = dt;
                d.f = i;
                d.g = guid;
                d.ID = i;
                d.iu = 10;
                d.l = i;
                d.s = 1;
                d.sb = 1;
                d.ts = tspan;
                d.ul = 10;
                d.us = 1;
                d.enn = myEnum.unu;
                d.str = "Abramé";


                sq.StoreObject(d);
            }

            sq.DropType<ClassIndexes>();
            for (var i = 0; i < 100; i++)
            {
                var cls = new ClassIndexes();
                cls.one = i % 10;
                cls.two = i % 10 + 1;
                cls.ID = i;
                cls.ID2 = i;
                sq.StoreObject(cls);
            }

            sq.Close();
            sq = new Siaqodb(objPath);
            byte byt = 5;
            var q1 = from D40WithIndexes di in sq
                where di.b == byt
                select di;

            Assert.AreEqual(1, q1.ToList().Count);

            var q2 = from D40WithIndexes di in sq
                where di.bo == true
                select di;

            Assert.AreEqual(10, q2.ToList().Count);

            var q3 = from D40WithIndexes di in sq
                where di.c == 'c'
                select di;

            Assert.AreEqual(10, q3.ToList().Count);

            var q4 = from D40WithIndexes di in sq
                where di.d == 5
                select di;

            Assert.AreEqual(1, q4.ToList().Count);

            var q5 = from D40WithIndexes di in sq
                where di.de == 5
                select di;

            Assert.AreEqual(1, q5.ToList().Count);

            var q6 = from D40WithIndexes di in sq
                where di.dt == dt
                select di;

            Assert.AreEqual(10, q6.ToList().Count);

            var q7 = from D40WithIndexes di in sq
                where di.enn == myEnum.unu
                select di;

            Assert.AreEqual(10, q7.ToList().Count);

            var q8 = from D40WithIndexes di in sq
                where di.f == 6
                select di;

            Assert.AreEqual(1, q8.ToList().Count);

            var q9 = from D40WithIndexes di in sq
                where di.g == guid
                select di;

            Assert.AreEqual(10, q9.ToList().Count);

            var q10 = from D40WithIndexes di in sq
                where di.iu == 10
                select di;

            Assert.AreEqual(10, q10.ToList().Count);

            var q11 = from D40WithIndexes di in sq
                where di.l == 7
                select di;

            Assert.AreEqual(1, q11.ToList().Count);

            var q12 = from D40WithIndexes di in sq
                where di.s == 1
                select di;

            Assert.AreEqual(10, q12.ToList().Count);

            var q13 = from D40WithIndexes di in sq
                where di.sb == 1
                select di;

            Assert.AreEqual(10, q13.ToList().Count);

            var q14 = from D40WithIndexes di in sq
                where di.str.StartsWith("Abr")
                select di;

            Assert.AreEqual(10, q14.ToList().Count);

            var q15 = from D40WithIndexes di in sq
                where di.ts == tspan
                select di;

            Assert.AreEqual(10, q15.ToList().Count);

            var q16 = from D40WithIndexes di in sq
                where di.ul == 10
                select di;

            Assert.AreEqual(10, q16.ToList().Count);

            var q17 = from D40WithIndexes di in sq
                where di.us == 1
                select di;

            Assert.AreEqual(10, q17.ToList().Count);

            var q18 = from ClassIndexes clss in sq
                where clss.two == 7
                select clss;

            Assert.AreEqual(10, q18.ToList().Count);

            var q19 = from D40WithIndexes di in sq
                where di.Text == "text longgg"
                select di;

            Assert.AreEqual(10, q19.ToList().Count);
        }

        [Test]
        public void TestAttributesOnProps()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ClassWithPropertiesAtt>();
            for (var i = 0; i < 10; i++)
            {
                var cls = new ClassWithPropertiesAtt();
                cls.ID = i % 2;
                cls.MyProperty = i + 1;
                cls.Stringss = "dsdsdsds";
                cls.Uniq = i;
                sq.StoreObject(cls);
            }


            var q = from ClassWithPropertiesAtt clss in sq
                where clss.ID == 1
                select clss;

            Assert.AreEqual(5, q.Count());
            //check ignore work
            Assert.AreEqual(0, q.ToList()[0].MyProperty);

            Assert.AreEqual(3, q.ToList()[0].Stringss.Length);

            q.ToList()[0].Uniq = 0;
            var except = false;
            try
            {
                sq.StoreObject(q.ToList()[0]);
            }
            catch (UniqueConstraintException ex)
            {
                except = true;
            }

            Assert.AreEqual(true, except);
        }

        [Test]
        public void TestPOCO()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<POCO>();
            for (var i = 0; i < 10; i++)
            {
                var cls = new POCO();
                cls.ID = i % 2;
                cls.MyProperty = i + 1;
                cls.Stringss = "dsdsdsds";
                cls.Uniq = i;
                sq.StoreObject(cls);
            }


            var q = from POCO clss in sq
                where clss.ID == 1
                select clss;

            Assert.AreEqual(5, q.Count());
            //check ignore work
            Assert.AreEqual(0, q.ToList()[0].MyProperty);

            Assert.AreEqual(3, q.ToList()[0].Stringss.Length);

            q.ToList()[0].Uniq = 0;
            var except = false;
            try
            {
                sq.StoreObject(q.ToList()[0]);
            }
            catch (UniqueConstraintException ex)
            {
                except = true;
            }

            Assert.AreEqual(true, except);
        }

        [Test]
        public void TestRealPOCO()
        {
            SiaqodbConfigurator.AddIndex("ID", typeof(RealPOCO));
            SiaqodbConfigurator.AddIndex("ID", typeof(RealPOCO1));

            SiaqodbConfigurator.AddUniqueConstraint("UID", typeof(RealPOCO));
            SiaqodbConfigurator.AddUniqueConstraint("UID", typeof(RealPOCO1));

            SiaqodbConfigurator.AddIgnore("ignoredField", typeof(RealPOCO));
            SiaqodbConfigurator.AddIgnore("ignoredField", typeof(RealPOCO1));

            SiaqodbConfigurator.AddIgnore("IgnoredProp", typeof(RealPOCO));
            SiaqodbConfigurator.AddIgnore("IgnoredProp", typeof(RealPOCO1));

            SiaqodbConfigurator.AddMaxLength("MyStr", 3, typeof(RealPOCO));
            SiaqodbConfigurator.AddMaxLength("MyStr", 3, typeof(RealPOCO1));

            SiaqodbConfigurator.AddMaxLength("mystr", 3, typeof(RealPOCO));
            SiaqodbConfigurator.AddMaxLength("mystr", 3, typeof(RealPOCO1));

            SiaqodbConfigurator.PropertyUseField("MyStrProp", "mystr", typeof(RealPOCO));
            SiaqodbConfigurator.PropertyUseField("MyStrProp", "mystr", typeof(RealPOCO1));

            var sq = new Siaqodb(objPath);
            sq.DropType<RealPOCO>();
            for (var i = 0; i < 10; i++)
            {
                var cls = new RealPOCO();
                cls.ID = i % 2;
                cls.Test = i + 1;
                cls.UID = Guid.NewGuid();
                cls.ignoredField = i;
                cls.IgnoredProp = i;
                cls.mystr = "dqwsdasdasdas";
                cls.MyStr = "dqwqwdqad";
                sq.StoreObject(cls);
            }


            var q = from RealPOCO clss in sq
                where clss.ID == 1
                select clss;

            Assert.AreEqual(5, q.Count());

            sq.Close();

            sq = new Siaqodb(objPath);
            q = from RealPOCO clss in sq
                where clss.ID == 1
                select clss;

            Assert.AreEqual(5, q.Count());

            var o1 = q.ToList()[0];
            var o2 = q.ToList()[1];

            //check if ignore work
            Assert.AreEqual(0, o1.ignoredField);
            Assert.AreEqual(0, o1.IgnoredProp);

            //check maxLength work
            Assert.AreEqual(3, o1.MyStr.Length);
            Assert.AreEqual(3, o1.mystr.Length);


            o2.UID = o1.UID;
            var excp = false;
            try
            {
                sq.StoreObject(o2);
            }
            catch (UniqueConstraintException ex)
            {
                excp = true;
            }

            Assert.AreEqual(true, excp);

            //check if mapping works
            q = from RealPOCO clss in sq
                where clss.MyStrProp == "dqw"
                select clss;

            Assert.AreEqual(10, q.ToList().Count);
        }

        [Test]
        public void TestOptimisticConcurency()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ForConcurencyClass>();
            var lis = new List<ForConcurencyClass>();
            for (var i = 0; i < 10; i++)
            {
                var c = new ForConcurencyClass();
                c.integ = i + 1;
                c.test = "test";
                sq.StoreObject(c);
                lis.Add(c);
            }

            var q = from ForConcurencyClass cc in sq
                select cc;
            var de = q.ToList()[0];

            de.test = "d";
            sq.StoreObject(de);
            var exCatched = 0;
            try
            {
                sq.StoreObject(lis[0]);
            }
            catch (OptimisticConcurrencyException e)
            {
                exCatched++;
            }

            Assert.AreEqual(1, exCatched);

            sq.StoreObject(de);

            q = from ForConcurencyClass cc in sq
                select cc;
            var de2 = q.ToList()[0];

            sq.StoreObject(de2);

            var newObj = new ForConcurencyClass();
            newObj.integ = 1;

            sq.UpdateObjectBy("integ", newObj);
            exCatched = 0;
            try
            {
                sq.StoreObject(de2);
            }
            catch (OptimisticConcurrencyException e)
            {
                exCatched++;
            }

            Assert.AreEqual(1, exCatched);

            sq.StoreObject(newObj);

            q = from ForConcurencyClass cc in sq
                select cc;
            var de3 = q.ToList()[0];

            sq.Delete(newObj);

            exCatched = 0;
            try
            {
                sq.StoreObject(de3);
            }
            catch (OptimisticConcurrencyException e)
            {
                exCatched++;
            }

            Assert.AreEqual(1, exCatched);

            q = from ForConcurencyClass cc in sq
                select cc;
            var de4 = q.ToList()[0];

            var de4bis = q.ToList()[1];

            var q1 = from ForConcurencyClass cc in sq
                select cc;

            var de5 = q1.ToList()[0];

            sq.StoreObject(de4);

            exCatched = 0;
            try
            {
                sq.Delete(de5);
            }
            catch (OptimisticConcurrencyException e)
            {
                exCatched++;
            }

            Assert.AreEqual(1, exCatched);

            var de6 = new ForConcurencyClass();
            de6.integ = 3;


            sq.DeleteObjectBy("integ", de6);


            exCatched = 0;
            try
            {
                sq.StoreObject(de4bis);
            }
            catch (OptimisticConcurrencyException e)
            {
                exCatched++;
            }

            Assert.AreEqual(1, exCatched);
        }

        [Test]
        public void TestTransactionInsert()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<Customer>();
            IList<Customer> list = null;
            var transact = sq.BeginTransaction();
            try
            {
                for (var i = 0; i < 10; i++)
                {
                    var c = new Customer();
                    c.Name = "GTA" + i;
                    sq.StoreObject(c, transact);
                }

                list = sq.LoadAll<Customer>();
                Assert.AreEqual(0, list.Count);


                transact.Commit();
            }
            catch (Exception ex)
            {
                transact.Rollback();
            }

            list = sq.LoadAll<Customer>();
            Assert.AreEqual(10, list.Count);
            sq.Close();
            sq.Open(objPath);
            list = sq.LoadAll<Customer>();
            Assert.AreEqual(10, list.Count);


            transact = sq.BeginTransaction();
            try
            {
                for (var i = 0; i < 10; i++)
                {
                    var c = new Customer();
                    c.Name = "GTA" + i;
                    sq.StoreObject(c, transact);
                    if (i == 9) throw new Exception("fsdfsd");
                }

                transact.Commit();
            }
            catch (Exception ex)
            {
                transact.Rollback();
            }

            list = sq.LoadAll<Customer>();
            Assert.AreEqual(10, list.Count);
        }

        [Test]
        public void TestTransactionUpdateInsert()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<Customer>();
            IList<Customer> list = null;
            var transact = sq.BeginTransaction();

            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.Name = "GTA" + i;
                sq.StoreObject(c); //without transact
            }

            list = sq.LoadAll<Customer>();
            Assert.AreEqual(10, list.Count);

            foreach (var c in list)
            {
                c.Name = "updated";
                sq.StoreObject(c, transact);
            }

            list = sq.LoadAll<Customer>();
            foreach (var c in list) Assert.AreEqual("GTA", c.Name.Substring(0, 3));
            try
            {
                transact.Commit();
            }
            catch (Exception ex)
            {
                transact.Rollback(); //problem with OptimistiConcurency
            }

            list = sq.LoadAll<Customer>();

            foreach (var c in list) Assert.AreEqual("updated", c.Name);
        }

        [Test]
        public void TestTransactionDelete()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<Customer>();
            IList<Customer> list = null;
            var transact = sq.BeginTransaction();

            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.Name = "GTA" + i;
                sq.StoreObject(c); //without transact
            }

            list = sq.LoadAll<Customer>();
            sq.Delete(list[0], transact);
            sq.Delete(list[1], transact);
            var rollback = false;
            try
            {
                transact.Commit();
            }
            catch
            {
                transact.Rollback();
                list = sq.LoadAll<Customer>();
                Assert.AreEqual(10, list.Count);
                rollback = true;
            }

            if (!rollback)
            {
                list = sq.LoadAll<Customer>();
                Assert.AreEqual(8, list.Count);
            }
        }

        [Test]
        public void TestUpdateObjectByManyFieldsTransaction()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<Employee>();

            var emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "s";
            sq.StoreObject(emp);


            emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "shuhu";

            var s = sq.UpdateObjectBy(emp, "ID", "CustomerID");

            Assert.IsTrue(s);
            IList<Employee> list = sq.LoadAll<Employee>();
            Assert.AreEqual(list[0].Name, emp.Name);

            emp.Name = "ANOTHER";
            var tr = sq.BeginTransaction();
            sq.UpdateObjectBy(emp, tr, "ID", "CustomerID");

            tr.Commit();
            list = sq.LoadAll<Employee>();
            Assert.AreEqual(list[0].Name, emp.Name);

            tr = sq.BeginTransaction();
            emp.Name = "test";

            sq.UpdateObjectBy(emp, tr, "ID", "CustomerID");

            tr.Rollback();
            list = sq.LoadAll<Employee>();
            Assert.AreEqual(list[0].Name, "ANOTHER");
        }

        [Test]
        public void TestDeleteObjectByTransactions()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<Employee>();

            var emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "s";
            sq.StoreObject(emp);


            emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;

            var trans = sq.BeginTransaction();

            var s = sq.DeleteObjectBy(emp, trans, "ID", "CustomerID");
            Assert.IsTrue(s);
            trans.Commit();
            IList<Employee> list = sq.LoadAll<Employee>();
            Assert.AreEqual(list.Count, 0);

            emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "s";
            sq.StoreObject(emp);


            trans = sq.BeginTransaction();
            s = sq.DeleteObjectBy(emp, trans, "ID", "CustomerID");
            trans.Rollback();

            list = sq.LoadAll<Employee>();
            Assert.AreEqual(list.Count, 1);

            emp = new Employee();
            emp.ID = 100;
            emp.CustomerID = 30;
            emp.Name = "s";
            sq.StoreObject(emp);


            trans = sq.BeginTransaction();
            try
            {
                s = sq.DeleteObjectBy(emp, trans, "ID", "CustomerID");


                trans.Commit();
            }
            catch
            {
                trans.Rollback();
            }

            list = sq.LoadAll<Employee>();
            Assert.AreEqual(list.Count, 2);
        }

        [Test]
        public void TestTransactionCrash()
        {
            var sq = new Siaqodb(objPath);

            IList<Customer> list = sq.LoadAll<Customer>();
            IList<Employee> list2 = sq.LoadAll<Employee>();

            sq.DropType<Customer>();
            sq.DropType<Employee>();

            var transact = sq.BeginTransaction();

            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.Name = "GTA" + i;
                sq.StoreObject(c, transact);
                var e = new Employee();
                e.Name = "EMP" + i;
                sq.StoreObject(e, transact);
            }

            transact.Commit();


            list = sq.LoadAll<Customer>();
            Assert.AreEqual(10, list.Count);

            list2 = sq.LoadAll<Employee>();
            Assert.AreEqual(10, list2.Count);

            var transac2t = sq.BeginTransaction();

            sq.Delete(list[5], transac2t);
            sq.Delete(list2[5], transac2t);

            for (var i = 0; i < 4; i++)
            {
                list[i].Name = "updated";
                list2[i].Name = "updatedE";
                sq.StoreObject(list[i], transac2t);
                sq.StoreObject(list2[i], transac2t);
                sq.StoreObject(new Customer(), transac2t);
                sq.StoreObject(new Employee(), transac2t);
            }


            transac2t.Commit(); //here do debug and stop after a few commits to be able to simulate crash recovery
        }

        [Test]
        public void TestTransactionManyTypes()
        {
            var sq = new Siaqodb(objPath);


            sq.DropType<Customer>();
            sq.DropType<Employee>();
            sq.DropType<D40>();
            var transact = sq.BeginTransaction();

            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.Name = "GTA" + i;
                sq.StoreObject(c, transact);
                var e = new Employee();
                e.Name = "EMP" + i;
                sq.StoreObject(e, transact);

                var d = new D40();
                sq.StoreObject(d, transact);
            }

            transact.Commit();


            IList<Customer> list = sq.LoadAll<Customer>();
            Assert.AreEqual(10, list.Count);

            IList<Employee> list2 = sq.LoadAll<Employee>();
            Assert.AreEqual(10, list2.Count);

            IList<D40> list3 = sq.LoadAll<D40>();
            Assert.AreEqual(10, list3.Count);

            var transac2t = sq.BeginTransaction();

            sq.Delete(list[5], transac2t);
            sq.Delete(list2[5], transac2t);
            sq.Delete(list3[5], transac2t);

            for (var i = 0; i < 4; i++)
            {
                list[i].Name = "updated";
                list2[i].Name = "updatedE";
                sq.StoreObject(list[i], transac2t);
                sq.StoreObject(list2[i], transac2t);
                sq.StoreObject(new Customer(), transac2t);
                sq.StoreObject(new Employee(), transac2t);
            }


            transac2t.Commit();

            list = sq.LoadAll<Customer>();
            Assert.AreEqual(13, list.Count);

            list2 = sq.LoadAll<Employee>();
            Assert.AreEqual(13, list2.Count);

            list3 = sq.LoadAll<D40>();
            Assert.AreEqual(9, list3.Count);
            Assert.AreEqual(list[0].Name, "updated");
            Assert.AreEqual(list2[0].Name, "updatedE");

            transac2t = sq.BeginTransaction();

            sq.Delete(list[5], transac2t);
            sq.Delete(list2[5], transac2t);
            sq.Delete(list3[5], transac2t);

            for (var i = 0; i < 4; i++)
            {
                list[i].Name = "updatedRoll";
                list2[i].Name = "updatedERoll";
                sq.StoreObject(list[i], transac2t);
                sq.StoreObject(list2[i], transac2t);
                sq.StoreObject(new Customer(), transac2t);
                sq.StoreObject(new Employee(), transac2t);
            }

            transac2t.Rollback();

            list = sq.LoadAll<Customer>();
            Assert.AreEqual(13, list.Count);

            list2 = sq.LoadAll<Employee>();
            Assert.AreEqual(13, list2.Count);

            list3 = sq.LoadAll<D40>();
            Assert.AreEqual(9, list3.Count);

            Assert.AreEqual(list[0].Name, "updated");
            Assert.AreEqual(list2[0].Name, "updatedE");
        }

        [Test]
        public void TestTransactionLists()
        {
            var sq = new Siaqodb(objPath);


            sq.DropType<Customer>();
            sq.DropType<Employee>();
            sq.DropType<D40WithLists>();
            var transact = sq.BeginTransaction();

            for (var i = 0; i < 10; i++)
            {
                var c = new Customer();
                c.Name = "GTA" + i;
                sq.StoreObject(c, transact);
                var e = new Employee();
                e.Name = "EMP" + i;
                sq.StoreObject(e, transact);

                var d = new D40WithLists();
                d.dt = new List<DateTime>();
                d.dt.Add(DateTime.Now);
                sq.StoreObject(d, transact);
            }

            transact.Commit();


            IList<Customer> list = sq.LoadAll<Customer>();
            Assert.AreEqual(10, list.Count);

            IList<Employee> list2 = sq.LoadAll<Employee>();
            Assert.AreEqual(10, list2.Count);

            IList<D40WithLists> list3 = sq.LoadAll<D40WithLists>();
            Assert.AreEqual(10, list3.Count);

            var transac2t = sq.BeginTransaction();

            sq.Delete(list[5], transac2t);
            sq.Delete(list2[5], transac2t);
            sq.Delete(list3[5], transac2t);

            for (var i = 0; i < 4; i++)
            {
                list[i].Name = "updated";
                list2[i].Name = "updatedE";
                list3[i].dt[0] = new DateTime(2007, 1, 1);
                sq.StoreObject(list[i], transac2t);
                sq.StoreObject(list2[i], transac2t);
                sq.StoreObject(list3[i], transac2t);
                sq.StoreObject(new Customer(), transac2t);
                sq.StoreObject(new Employee(), transac2t);
                sq.StoreObject(new D40WithLists(), transac2t);
            }


            transac2t.Commit();

            list = sq.LoadAll<Customer>();
            Assert.AreEqual(13, list.Count);

            list2 = sq.LoadAll<Employee>();
            Assert.AreEqual(13, list2.Count);

            list3 = sq.LoadAll<D40WithLists>();
            Assert.AreEqual(13, list3.Count);
            Assert.AreEqual(list[0].Name, "updated");
            Assert.AreEqual(list2[0].Name, "updatedE");
            Assert.AreEqual(list3[0].dt[0], new DateTime(2007, 1, 1));

            transac2t = sq.BeginTransaction();

            sq.Delete(list[5], transac2t);
            sq.Delete(list2[5], transac2t);
            sq.Delete(list3[5], transac2t);

            for (var i = 0; i < 4; i++)
            {
                list[i].Name = "updatedRoll";
                list2[i].Name = "updatedERoll";
                list3[i].dt[0] = new DateTime(2008, 3, 3);
                sq.StoreObject(list[i], transac2t);
                sq.StoreObject(list2[i], transac2t);
                sq.StoreObject(new Customer(), transac2t);
                sq.StoreObject(new Employee(), transac2t);

                sq.StoreObject(list3[i], transac2t);
            }

            transac2t.Rollback();

            list = sq.LoadAll<Customer>();
            Assert.AreEqual(13, list.Count);

            list2 = sq.LoadAll<Employee>();
            Assert.AreEqual(13, list2.Count);

            list3 = sq.LoadAll<D40WithLists>();
            Assert.AreEqual(13, list3.Count);

            Assert.AreEqual(list[0].Name, "updated");
            Assert.AreEqual(list2[0].Name, "updatedE");
            Assert.AreEqual(list3[0].dt[0], new DateTime(2007, 1, 1));
        }

        [Test]
        public void TestIndexStringStartWith()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<ClassIndexesString>();
            var cls = new ClassIndexesString { Name = "aaa" };
            sq.StoreObject(cls);

            cls = new ClassIndexesString { Name = "bbabyb" };
            sq.StoreObject(cls);

            cls = new ClassIndexesString { Name = "bba" };
            sq.StoreObject(cls);

            cls = new ClassIndexesString { Name = "bbazz" };
            sq.StoreObject(cls);

            cls = new ClassIndexesString { Name = "ab" };
            sq.StoreObject(cls);

            cls = new ClassIndexesString { Name = "rere" };
            sq.StoreObject(cls);
            cls = new ClassIndexesString { Name = "abbb" };
            sq.StoreObject(cls);
            //sq.Close();
            //sq = new Siaqodb(objPath);
            var q = (from ClassIndexesString clss in sq
                where clss.Name.StartsWith("bb")
                select clss).ToList();

            Assert.AreEqual(3, q.Count);
            foreach (var hu in q) Assert.IsTrue(hu.Name.StartsWith("bb"));
        }

        [Test]
        public void TestListsAllTypes()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<D40WithLists>();

            var dt = new DateTime(2010, 1, 1);
            var guid = Guid.NewGuid();
            var tspan = new TimeSpan();
            for (var i = 0; i < 10; i++)
            {
                var d = new D40WithLists();
                d.b = new List<byte>();
                d.b.Add(Convert.ToByte(i));

                d.bo = new[] { true, false };
                d.c = new[] { 'c', 'd' };
                d.d = new double[] { i, i };
                d.de = new decimal[] { i, i };
                d.dt = new List<DateTime>();
                d.dt.Add(dt);
                d.f = new float[] { i, i };
                d.g = new List<Guid>();
                d.g.Add(guid);
                d.ID = i;
                d.iu = new List<uint>();
                d.iu.Add(10);
                d.l = null;
                d.s = new List<short>();
                d.s.Add(1);
                d.sb = new List<sbyte>();
                d.sb.Add(1);
                d.ts = new List<TimeSpan>();
                d.ts.Add(tspan);
                d.ul = new List<ulong>();
                d.ul.Add(10);
                d.us = new List<ushort>();
                d.enn = new List<myEnum>();
                d.enn.Add(myEnum.unu);
                d.str = new List<string>();
                d.str.Add("Abramé");
                d.textList = new List<string>();
                d.textList.Add(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaalllllllbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccc44444444444444444444477777777777777777777777");
                sq.StoreObject(d);
            }

            var all1 = sq.LoadAll<D40WithLists>();
            var ii = 0;
            foreach (var dL in all1)
            {
                Assert.AreEqual(Convert.ToByte(ii), dL.b[0]);
                Assert.AreEqual(true, dL.bo[0]);
                Assert.AreEqual(false, dL.bo[1]);
                Assert.AreEqual('c', dL.c[0]);
                Assert.AreEqual('d', dL.c[1]);
                Assert.AreEqual(ii, dL.d[1]);
                Assert.AreEqual(ii, dL.de[0]);

                Assert.AreEqual(dt, dL.dt[0]);
                Assert.AreEqual(ii, dL.f[0]);
                Assert.AreEqual(guid, dL.g[0]);
                Assert.AreEqual((uint)10, dL.iu[0]);
                Assert.AreEqual(null, dL.l);
                Assert.AreEqual((short)1, dL.s[0]);
                Assert.AreEqual((sbyte)1, dL.sb[0]);
                Assert.AreEqual(tspan, dL.ts[0]);
                Assert.AreEqual((ulong)10, dL.ul[0]);
                Assert.AreEqual(0, dL.us.Count);
                Assert.AreEqual(myEnum.unu, dL.enn[0]);
                Assert.AreEqual("Abramé", dL.str[0]);
                Assert.AreEqual(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaalllllllbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccc44444444444444444444477777777777777777777777",
                    dL.textList[0]);
                ii++;
            }

            var q21 = (from D40WithLists dll in sq
                where dll.g.Contains(guid)
                select dll).ToList();

            Assert.AreEqual(10, q21.Count);
            sq.Close();
            sq = new Siaqodb(objPath);
            all1 = sq.LoadAll<D40WithLists>();
            ii = 0;
            foreach (var dL in all1)
            {
                Assert.AreEqual(Convert.ToByte(ii), dL.b[0]);
                Assert.AreEqual(true, dL.bo[0]);
                Assert.AreEqual(false, dL.bo[1]);
                Assert.AreEqual('c', dL.c[0]);
                Assert.AreEqual('d', dL.c[1]);
                Assert.AreEqual(ii, dL.d[1]);
                Assert.AreEqual(ii, dL.de[0]);

                Assert.AreEqual(dt, dL.dt[0]);
                Assert.AreEqual(ii, dL.f[0]);
                Assert.AreEqual(guid, dL.g[0]);
                Assert.AreEqual((uint)10, dL.iu[0]);
                Assert.AreEqual(null, dL.l);
                Assert.AreEqual((short)1, dL.s[0]);
                Assert.AreEqual((sbyte)1, dL.sb[0]);
                Assert.AreEqual(tspan, dL.ts[0]);
                Assert.AreEqual((ulong)10, dL.ul[0]);
                Assert.AreEqual(0, dL.us.Count);
                Assert.AreEqual(myEnum.unu, dL.enn[0]);
                Assert.AreEqual("Abramé", dL.str[0]);
                Assert.AreEqual(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaalllllllbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccc44444444444444444444477777777777777777777777",
                    dL.textList[0]);
                ii++;
            }
        }

        [Test]
        public void TestOpen2Databases()
        {
            var s1 = new Siaqodb(@"F:\demo\s1\");
            s1.DropType<POCO>();

            for (var i = 0; i < 10; i++)
            {
                var pp = new POCO();
                pp.Uniq = i;
                s1.StoreObject(pp);
            }

            s1.Flush();

            var s2 = new Siaqodb(@"F:\demo\s2\");

            IList<POCO> poc1 = s1.LoadAll<POCO>();

            Assert.AreEqual(10, poc1.Count);
            IList<POCO> poc2 = s2.LoadAll<POCO>();

            Assert.AreEqual(0, poc2.Count);
        }

        [Test]
        public void TestLoadingEvents()
        {
            //SiaqodbConfigurator.SetRaiseLoadEvents(true);
            var sq = new Siaqodb(objPath);
            sq.LoadingObject += sq_LoadingObject;
            sq.LoadedObject += sq_LoadedObject;
            sq.DropType<POCO>();
            for (var i = 0; i < 10; i++)
            {
                var cls = new POCO();
                cls.ID = i % 2;
                cls.MyProperty = i + 1;
                cls.Stringss = "dsdsdsds";
                cls.Uniq = i;
                sq.StoreObject(cls);
            }

            IList<POCO> all = sq.LoadAll<POCO>();
        }

        private void sq_LoadedObject(object sender, LoadedObjectEventArgs e)
        {
        }

        private void sq_LoadingObject(object sender, LoadingObjectEventArgs e)
        {
        }

        [Test]
        public void TestNestedSelfObject()
        {
            //SiaqodbConfigurator.SetRaiseLoadEvents(true);
            var sq = new Siaqodb(objPath);

            sq.DropType<Person>();
            for (var i = 0; i < 10; i++)
            {
                var p = new Person();
                p.Name = i.ToString();
                p.friend = new Person();
                p.friend.Name = (i + 10).ToString();
                sq.StoreObject(p);
            }

            IList<Person> all = sq.LoadAll<Person>();
            Assert.AreEqual(20, all.Count);
            var j = 0;
            for (var i = 0; i < 20; i++)
                if (i % 2 == 0)
                {
                    Assert.AreEqual(j.ToString(), all[i].Name);
                    Assert.AreEqual((j + 10).ToString(), all[i].friend.Name);
                    j++;
                }
                else
                {
                    Assert.IsNull(all[i].friend);
                }
        }

        [Test]
        public void TestDateTimeKind()
        {
            SiaqodbConfigurator.SpecifyStoredDateTimeKind(DateTimeKind.Utc);
            var sq = new Siaqodb(objPath);

            sq.DropType<D40>();

            var p = new D40();
            p.dt = DateTime.Now;
            sq.StoreObject(p);

            IList<D40> lis = sq.LoadAll<D40>();
            Assert.AreEqual(DateTimeKind.Utc, lis[0].dt.Kind);

            SiaqodbConfigurator.SpecifyStoredDateTimeKind(DateTimeKind.Local);
            p = new D40();
            p.dt = DateTime.Now;
            sq.StoreObject(p);

            lis = sq.LoadAll<D40>();
            Assert.AreEqual(DateTimeKind.Local, lis[0].dt.Kind);
            Assert.AreEqual(DateTimeKind.Local, lis[1].dt.Kind);

            SiaqodbConfigurator.SpecifyStoredDateTimeKind(null);
            p = new D40();
            p.dt = DateTime.Now;
            sq.StoreObject(p);

            lis = sq.LoadAll<D40>();
            Assert.AreEqual(DateTimeKind.Unspecified, lis[0].dt.Kind);
            Assert.AreEqual(DateTimeKind.Unspecified, lis[1].dt.Kind);
            Assert.AreEqual(DateTimeKind.Unspecified, lis[2].dt.Kind);
        }

        [Test]
        public void TestShrink()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<D40WithLists>();

            var dt = new DateTime(2010, 1, 1);
            var guid = Guid.NewGuid();
            var tspan = new TimeSpan();
            for (var i = 0; i < 10; i++)
            {
                var d = new D40WithLists();
                d.b = new List<byte>();
                d.b.Add(Convert.ToByte(i));

                d.bo = new[] { true, false };
                d.c = new[] { 'c', 'd' };
                d.d = new double[] { i, i };
                d.de = new decimal[] { i, i };
                d.dt = new List<DateTime>();
                d.dt.Add(dt);
                d.f = new float[] { i, i };
                d.g = new List<Guid>();
                d.g.Add(guid);
                d.ID = i;
                d.iu = new List<uint>();
                d.iu.Add(10);
                d.l = null;
                d.s = new List<short>();
                d.s.Add(1);
                d.sb = new List<sbyte>();
                d.sb.Add(1);
                d.ts = new List<TimeSpan>();
                d.ts.Add(tspan);
                d.ul = new List<ulong>();
                d.ul.Add(10);
                d.us = new List<ushort>();
                d.enn = new List<myEnum>();
                d.enn.Add(myEnum.unu);
                d.str = new List<string>();
                d.str.Add("Abramé");

                sq.StoreObject(d);
            }

            var all = sq.LoadAll<D40WithLists>();
            for (var i = 5; i < 10; i++) sq.Delete(all[i]);
            sq.Close();

            SiaqodbUtil.Shrink(objPath, ShrinkType.Normal);
            SiaqodbUtil.Shrink(objPath, ShrinkType.ForceClaimSpace);

            sq = new Siaqodb(objPath);
            for (var i = 0; i < 10; i++)
            {
                var d = new D40WithLists();
                d.b = new List<byte>();
                d.b.Add(Convert.ToByte(i));

                d.bo = new[] { true, false };
                d.c = new[] { 'c', 'd' };
                d.d = new double[] { i, i };
                d.de = new decimal[] { i, i };
                d.dt = new List<DateTime>();
                d.dt.Add(dt);
                d.f = new float[] { i, i };
                d.g = new List<Guid>();
                d.g.Add(guid);
                d.ID = i;
                d.iu = new List<uint>();
                d.iu.Add(10);
                d.l = null;
                d.s = new List<short>();
                d.s.Add(1);
                d.sb = new List<sbyte>();
                d.sb.Add(1);
                d.ts = new List<TimeSpan>();
                d.ts.Add(tspan);
                d.ul = new List<ulong>();
                d.ul.Add(10);
                d.us = new List<ushort>();
                d.enn = new List<myEnum>();
                d.enn.Add(myEnum.unu);
                d.str = new List<string>();
                d.str.Add("Abramé");

                sq.StoreObject(d);
            }

            var all1 = sq.LoadAll<D40WithLists>();


            var ii = 0;
            var firstTime = false;
            foreach (var dL in all1)
            {
                if (ii == 5 && !firstTime)
                {
                    ii = 0;
                    firstTime = true;
                }

                Assert.AreEqual(Convert.ToByte(ii), dL.b[0]);
                Assert.AreEqual(true, dL.bo[0]);
                Assert.AreEqual(false, dL.bo[1]);
                Assert.AreEqual('c', dL.c[0]);
                Assert.AreEqual('d', dL.c[1]);
                Assert.AreEqual(ii, dL.d[1]);
                Assert.AreEqual(ii, dL.de[0]);

                Assert.AreEqual(dt, dL.dt[0]);
                Assert.AreEqual(ii, dL.f[0]);
                Assert.AreEqual(guid, dL.g[0]);
                Assert.AreEqual((uint)10, dL.iu[0]);
                Assert.AreEqual(null, dL.l);
                Assert.AreEqual((short)1, dL.s[0]);
                Assert.AreEqual((sbyte)1, dL.sb[0]);
                Assert.AreEqual(tspan, dL.ts[0]);
                Assert.AreEqual((ulong)10, dL.ul[0]);
                Assert.AreEqual(0, dL.us.Count);
                Assert.AreEqual(myEnum.unu, dL.enn[0]);
                Assert.AreEqual("Abramé", dL.str[0]);

                ii++;
            }

            var q21 = (from D40WithLists dll in sq
                where dll.g.Contains(guid)
                select dll).ToList();

            Assert.AreEqual(15, q21.Count);
        }

        [Test]
        public void TestIndexShrink()
        {
            var sq = new Siaqodb(objPath);
            sq.DropType<D40WithIndexes>();

            var dt = new DateTime(2010, 1, 1);
            var guid = Guid.NewGuid();
            var tspan = new TimeSpan();
            for (var i = 0; i < 10; i++)
            {
                var d = new D40WithIndexes();
                d.b = Convert.ToByte(i);

                d.bo = true;
                d.c = 'c';
                d.d = i;
                d.de = i;
                d.dt = dt;
                d.f = i;
                d.g = guid;
                d.ID = i;
                d.iu = 10;
                d.l = i;
                d.s = 1;
                d.sb = 1;
                d.ts = tspan;
                d.ul = 10;
                d.us = 1;
                d.enn = myEnum.unu;
                d.str = "Abramé";


                sq.StoreObject(d);
            }

            sq.DropType<ClassIndexes>();
            for (var i = 0; i < 100; i++)
            {
                var cls = new ClassIndexes();
                cls.one = i % 10;
                cls.two = i % 10 + 1;
                cls.ID = i;
                cls.ID2 = i;
                sq.StoreObject(cls);
            }

            IList<D40WithIndexes> all30 = sq.LoadAll<D40WithIndexes>();
            for (var i = 5; i < 10; i++) sq.Delete(all30[i]);
            sq.Close();

            SiaqodbUtil.Shrink(objPath, ShrinkType.Normal);
            SiaqodbUtil.Shrink(objPath, ShrinkType.ForceClaimSpace);

            sq = new Siaqodb(objPath);
            byte byt = 3;
            var q1 = from D40WithIndexes di in sq
                where di.b == byt
                select di;

            Assert.AreEqual(1, q1.ToList().Count);

            var q2 = from D40WithIndexes di in sq
                where di.bo == true
                select di;

            Assert.AreEqual(5, q2.ToList().Count);

            var q3 = from D40WithIndexes di in sq
                where di.c == 'c'
                select di;

            Assert.AreEqual(5, q3.ToList().Count);

            var q4 = from D40WithIndexes di in sq
                where di.d == 3
                select di;

            Assert.AreEqual(1, q4.ToList().Count);

            var q5 = from D40WithIndexes di in sq
                where di.de == 3
                select di;

            Assert.AreEqual(1, q5.ToList().Count);

            var q6 = from D40WithIndexes di in sq
                where di.dt == dt
                select di;

            Assert.AreEqual(5, q6.ToList().Count);

            var q7 = from D40WithIndexes di in sq
                where di.enn == myEnum.unu
                select di;

            Assert.AreEqual(5, q7.ToList().Count);

            var q8 = from D40WithIndexes di in sq
                where di.f == 3
                select di;

            Assert.AreEqual(1, q8.ToList().Count);

            var q9 = from D40WithIndexes di in sq
                where di.g == guid
                select di;

            Assert.AreEqual(5, q9.ToList().Count);

            var q10 = from D40WithIndexes di in sq
                where di.iu == 10
                select di;

            Assert.AreEqual(5, q10.ToList().Count);

            var q11 = from D40WithIndexes di in sq
                where di.l == 2
                select di;

            Assert.AreEqual(1, q11.ToList().Count);

            var q12 = from D40WithIndexes di in sq
                where di.s == 1
                select di;

            Assert.AreEqual(5, q12.ToList().Count);

            var q13 = from D40WithIndexes di in sq
                where di.sb == 1
                select di;

            Assert.AreEqual(5, q13.ToList().Count);

            var q14 = from D40WithIndexes di in sq
                where di.str.StartsWith("Abr")
                select di;

            Assert.AreEqual(5, q14.ToList().Count);

            var q15 = from D40WithIndexes di in sq
                where di.ts == tspan
                select di;

            Assert.AreEqual(5, q15.ToList().Count);

            var q16 = from D40WithIndexes di in sq
                where di.ul == 10
                select di;

            Assert.AreEqual(5, q16.ToList().Count);

            var q17 = from D40WithIndexes di in sq
                where di.us == 1
                select di;

            Assert.AreEqual(5, q17.ToList().Count);

            var q18 = from ClassIndexes clss in sq
                where clss.two == 7
                select clss;

            Assert.AreEqual(10, q18.ToList().Count);

            var q19 = from D40WithIndexes di in sq
                where di.Text == "text longgg"
                select di;

            Assert.AreEqual(5, q19.ToList().Count);
        }

        [Test]
        public void TestInsertBufferChunk()
        {
            SiaqodbConfigurator.BufferingChunkPercent = 2;
            var nop = new Siaqodb(objPath);
            nop.DropType<Customer>();

            for (var i = 0; i < 10000; i++)
            {
                var c = new Customer();
                c.ID = i;
                c.Name = "NOR" + i;

                nop.StoreObject(c);
            }

            nop.Flush();
            var listC = nop.LoadAll<Customer>();
            Assert.AreEqual(listC.Count, 10000);
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

    public class RealPOCO
    {
        public int ignoredField;
        public string mystr;
        public int Test;
        private ulong tickCount;
        public int ID { get; set; }

        public Guid UID { get; set; }


        public int IgnoredProp { get; set; }


        public string MyStr { get; set; }

        public string MyStrProp
        {
            get
            {
                Console.WriteLine("dsds");
                if (1 == 2) return null;
                return mystr;
            }
            set
            {
                Console.WriteLine("dsds");
                if (1 == 2) mystr = "d";
                mystr = value;
            }
        }
    }

    public class RealPOCO1
    {
        public int ignoredField;
        public string mystr;
        public int Test;
        private ulong tickCount;
        public int ID { get; set; }

        public Guid UID { get; set; }


        public int IgnoredProp { get; set; }


        public string MyStr { get; set; }

        public string MyStrProp
        {
            get
            {
                Console.WriteLine("dsds");
                if (1 == 2) return null;
                return mystr;
            }
            set
            {
                Console.WriteLine("dsds");
                if (1 == 2) mystr = "d";
                mystr = value;
            }
        }
    }

    public class POCO
    {
        public int Test;


        private ulong tickCount;

        [Index] public int ID { get; set; }

        [sqoDB.Attributes.Ignore] public int MyProperty { get; set; }

        [UniqueConstraint] public int Uniq { get; set; }

        [MaxLength(3)] public string Stringss { get; set; }
    }

    public class ClassWithPropertiesAtt : SqoDataObject
    {
        private ulong tickCount;

        [Index] public int ID { get; set; }

        [sqoDB.Attributes.Ignore] public int MyProperty { get; set; }

        [UniqueConstraint] public int Uniq { get; set; }

        [MaxLength(3)] public string Stringss { get; set; }
    }

    public class ClassIndexesString
    {
        [Index] public string Name;
    }

    public class ClassIndexes
    {
        [Index] public int ID;

        public int ID2;

        [Index] public int one;

        private ulong tickCount;

        [Index] public int two;
    }

    public class ClassWithEvents
    {
        public delegate void MyDelegate();

        public MyDelegate myDelegateMember;
        public int one;


        private ulong tickCount;
        public event EventHandler<EventArgs> MyCustomEvent;
    }

    public class ItemUnique
    {
        [UniqueConstraint] public int Age;

        public int integ;
        private int oid;

        [UniqueConstraint] public string S;

        private ulong tickCount;
    }

    public class Employee
    {
        public int CustomerID;
        public int ID;

        [MaxLength(20)] public string Name;

        private ulong tickCount;
        public string ENameProp => Name;

        public int OID { get; set; }
    }

    public class EmployeeLite
    {
        public int CustomerID;
        public TestEnum EmpEnum;
        public int ID;

        [MaxLength(20)] public string Name;


        private ulong tickCount;
    }

    public class Customer : SqoDataObject
    {
        [Index] public int ID;

        [MaxLength(20)] public string Name;

        public string stringWithoutAtt;


        private ulong tickCount;

        [UseVariable("ID")] public int IDProp => ID;

        public int IDPropWithoutAtt => ID;

        [UseVariable("IDs")]
        public int IDPropWithNonExistingVar
        {
            get
            {
                if (ID > 9) return 1;
                return -1;
            }
        }

        public bool IsTrue(string s)
        {
            return s == "ADH3";
        }
    }

    public class CustomerLite
    {
        private ulong tickCount;
        public string Name { get; set; }
        public int Age { get; set; }
        public bool Active { get; set; } = true;

        public TestEnum TEnum { get; set; }

        public int OID { get; set; }
    }

    public enum TestEnum
    {
        Unu,
        Doi,
        Trei
    }

    public class Order
    {
        public int EmployeeID;
        public int ID;

        [MaxLength(20)] public string Name;


        private ulong tickCount;
    }

    public class EmpCust
    {
        public string CName;
        public string EName;
        private ulong tickCount;
    }

    public class Something
    {
        public int one;
        private ulong tickCount;
        public int two;
    }

    public class Something32
    {
        public int one;

        public long three;
        public int three1;
        public int three2;


        private ulong tickCount;
    }

    public class EmpCustOID
    {
        public string CName;
        public string EName;
        public int EOID;
        private ulong tickCount;
    }

    public class D40
    {
        public byte b;
        public bool bo;
        public char c;
        public double d;
        public decimal de;
        public DateTime dt;
        public DateTimeOffset dtsofs;
        public myEnum enn = myEnum.doi;
        public float f;
        public Guid g;
        public int i;
        public int ID;
        public uint iu;
        public long l;

        public short s;
        public sbyte sb;

        [MaxLength(20)] public string str = "test";

        [Text] public string Text;

        private ulong tickCount;
        public TimeSpan ts;
        public ulong ul;
        public ushort us;
    }

    public class D40Nullable
    {
        public byte? b;
        public bool? bo;
        public char? c;
        public double? d;
        public decimal? de;
        public DateTime? dt;
        public DateTimeOffset? dtsofs;
        public myEnum enn = myEnum.doi;
        public float? f;
        public Guid? g;
        public int? i;
        public int? ID;
        public uint? iu;
        public long? l;

        public short? s;
        public sbyte? sb;

        [MaxLength(20)] public string str = "test";

        [Text] public string Text;

        private ulong tickCount;
        public TimeSpan? ts;
        public ulong? ul;
        public ushort? us;
    }

    public enum myEnum
    {
        unu = 2,
        doi
    }

    public class ForConcurencyClass
    {
        public int integ;
        public string test;
        private ulong tickCount;
    }

    public class D40WithIndexes
    {
        public byte b;

        [Index] public bool bo;

        [Index] public char c;

        [Index] public double d;

        [Index] public decimal de;

        [Index] public DateTime dt;

        [Index] public myEnum enn = myEnum.doi;

        [Index] public float f;

        [Index] public Guid g;

        [Index] public int i;

        public int ID;

        [Index] public uint iu;

        [Index] public long l;

        [Index] public short s;

        [Index] public sbyte sb;

        [MaxLength(20)] [Index] public string str = "test";

        [Text] [Index] public string Text = "text longgg";


        private ulong tickCount;

        [Index] public TimeSpan ts;

        [Index] public ulong ul;

        [Index] public ushort us;
    }

    public class D40WithLists
    {
        public List<byte> b;

        public bool[] bo;

        public char[] c;

        public double[] d;

        public decimal[] de;

        public List<DateTime> dt;

        public List<myEnum> enn;

        public float[] f;

        public List<Guid> g;

        public List<int> i;
        public int ID;

        public List<uint> iu;

        public List<long> l;

        public List<short> s;

        public List<sbyte> sb;

        [MaxLength(20)] public List<string> str;

        [Text] public List<string> textList;


        private ulong tickCount;

        public List<TimeSpan> ts;

        public List<ulong> ul;

        public List<ushort> us;
    }

    public class Person
    {
        public Person friend;

        public string Name;
    }
}