using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using sqoDB;

namespace sqoDBDB.Tests
{
    /// <summary>
    ///     Summary description for JaggedTests
    /// </summary>
    [TestFixture]
    public class JaggedTests
    {
        private readonly string objPath = TestUtils.GetTempPath();

        public JaggedTests()
        {
            SiaqodbConfigurator.EncryptedDatabase = true;
        }

        /// <summary>
        ///     Gets or sets the test context which provides
        ///     information about and functionality for the current test run.
        /// </summary>
        public TestContext TestContext { get; set; }

        [Test]
        public void Test1()
        {
            //
            // TODO: Add test logic here
            //
        }

        [Test]
        public void TestStoreSimpleJagged()
        {
            var s_db = new Siaqodb(objPath);


            s_db.DropType<JaggedTy>();
            for (var i = 0; i < 10; i++)
            {
                var jg = new JaggedTy();
                jg.jaggedByte = new byte[2][];
                jg.jaggedByte[0] = new byte[2];
                jg.jaggedByte[0][0] = (byte)i;
                jg.jaggedByte[0][1] = (byte)(i + 1);
                jg.jaggedByte[1] = new byte[2];
                jg.jaggedByte[1][0] = (byte)(i + 2);
                jg.jaggedByte[1][1] = (byte)(i + 3);

                jg.jaggedInt = new int[2][];
                jg.jaggedInt[0] = new int[2];
                jg.jaggedInt[0][0] = i;
                jg.jaggedInt[0][1] = i + 1;
                jg.jaggedInt[1] = new int[2];
                jg.jaggedInt[1][0] = i + 2;
                jg.jaggedInt[1][1] = i + 3;

                jg.jaggedList = new List<List<int>>();
                var myList = new List<int>();
                myList.Add(i);
                myList.Add(i + 1);
                var myList2 = new List<int>();
                myList2.Add(i * 10);
                myList2.Add(i * 100);
                jg.jaggedList.Add(myList);
                jg.jaggedList.Add(myList2);

                jg.jaggedListStr = new List<List<string>>();
                var myListStr = new List<string>();
                myListStr.Add("ws" + i);
                myListStr.Add("second" + i);
                jg.jaggedListStr.Add(myListStr);

                var simple = new SimpleClass();
                simple.Name = "ssss";
                jg.complexJaggedList = new List<List<SimpleClass>>();
                var listSimple = new List<SimpleClass>();
                listSimple.Add(simple);

                jg.complexJaggedList.Add(listSimple);
                jg.complexJaggedList.Add(listSimple);
                s_db.StoreObject(jg);
            }

            IList<JaggedTy> all = s_db.LoadAll<JaggedTy>();
            Assert.AreEqual(10, all.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual((byte)i, all[i].jaggedByte[0][0]);
                Assert.AreEqual((byte)(i + 1), all[i].jaggedByte[0][1]);
                Assert.AreEqual((byte)(i + 2), all[i].jaggedByte[1][0]);
                Assert.AreEqual((byte)(i + 3), all[i].jaggedByte[1][1]);

                Assert.AreEqual(i, all[i].jaggedInt[0][0]);
                Assert.AreEqual(i + 1, all[i].jaggedInt[0][1]);
                Assert.AreEqual(i + 2, all[i].jaggedInt[1][0]);
                Assert.AreEqual(i + 3, all[i].jaggedInt[1][1]);

                Assert.AreEqual(2, all[i].jaggedList[0].Count);
                Assert.AreEqual(2, all[i].jaggedList.Count);
                Assert.AreEqual(i, all[i].jaggedList[0][0]);
                Assert.AreEqual(i + 1, all[i].jaggedList[0][1]);
                Assert.AreEqual(i * 10, all[i].jaggedList[1][0]);
                Assert.AreEqual(i * 100, all[i].jaggedList[1][1]);

                Assert.AreEqual(1, all[i].jaggedListStr.Count);
                Assert.AreEqual(2, all[i].jaggedListStr[0].Count);
                Assert.AreEqual("ws" + i, all[i].jaggedListStr[0][0]);
                Assert.AreEqual("second" + i, all[i].jaggedListStr[0][1]);
                Assert.AreEqual(2, all[i].complexJaggedList.Count);
                Assert.IsNotNull(all[i].complexJaggedList[0][0]);
            }

            s_db.Close();
            s_db = new Siaqodb(objPath);
            all = s_db.LoadAll<JaggedTy>();
            Assert.AreEqual(10, all.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual((byte)i, all[i].jaggedByte[0][0]);
                Assert.AreEqual((byte)(i + 1), all[i].jaggedByte[0][1]);
                Assert.AreEqual((byte)(i + 2), all[i].jaggedByte[1][0]);
                Assert.AreEqual((byte)(i + 3), all[i].jaggedByte[1][1]);

                Assert.AreEqual(i, all[i].jaggedInt[0][0]);
                Assert.AreEqual(i + 1, all[i].jaggedInt[0][1]);
                Assert.AreEqual(i + 2, all[i].jaggedInt[1][0]);
                Assert.AreEqual(i + 3, all[i].jaggedInt[1][1]);

                Assert.AreEqual(2, all[i].jaggedList[0].Count);
                Assert.AreEqual(2, all[i].jaggedList.Count);
                Assert.AreEqual(i, all[i].jaggedList[0][0]);
                Assert.AreEqual(i + 1, all[i].jaggedList[0][1]);
                Assert.AreEqual(i * 10, all[i].jaggedList[1][0]);
                Assert.AreEqual(i * 100, all[i].jaggedList[1][1]);

                Assert.AreEqual(1, all[i].jaggedListStr.Count);
                Assert.AreEqual(2, all[i].jaggedListStr[0].Count);
                Assert.AreEqual("ws" + i, all[i].jaggedListStr[0][0]);
                Assert.AreEqual("second" + i, all[i].jaggedListStr[0][1]);
                Assert.AreEqual(2, all[i].complexJaggedList.Count);
                Assert.IsNotNull(all[i].complexJaggedList[0][0]);
            }

            var q = from JaggedTy jgd in s_db
                where jgd.jaggedInt.Length == 2
                select jgd;

            Assert.AreEqual(10, q.ToList().Count);

            var myInList = new List<int>();
            myInList.Add(0);
            myInList.Add(1);

            var q2 = from JaggedTy jgd in s_db
                where jgd.jaggedList.Contains(myInList)
                select jgd;

            Assert.AreEqual(1, q2.ToList().Count);
        }

        [Test]
        public void TestStoreSimpleJaggedAndShrink()
        {
            var s_db = new Siaqodb(objPath);


            s_db.DropType<JaggedTy>();
            for (var i = 0; i < 20; i++)
            {
                var jg = new JaggedTy();
                jg.jaggedByte = new byte[2][];
                jg.jaggedByte[0] = new byte[2];
                jg.jaggedByte[0][0] = (byte)i;
                jg.jaggedByte[0][1] = (byte)(i + 1);
                jg.jaggedByte[1] = new byte[2];
                jg.jaggedByte[1][0] = (byte)(i + 2);
                jg.jaggedByte[1][1] = (byte)(i + 3);

                jg.jaggedInt = new int[2][];
                jg.jaggedInt[0] = new int[2];
                jg.jaggedInt[0][0] = i;
                jg.jaggedInt[0][1] = i + 1;
                jg.jaggedInt[1] = new int[2];
                jg.jaggedInt[1][0] = i + 2;
                jg.jaggedInt[1][1] = i + 3;

                jg.jaggedList = new List<List<int>>();
                var myList = new List<int>();
                myList.Add(i);
                myList.Add(i + 1);
                var myList2 = new List<int>();
                myList2.Add(i * 10);
                myList2.Add(i * 100);
                jg.jaggedList.Add(myList);
                jg.jaggedList.Add(myList2);

                jg.jaggedListStr = new List<List<string>>();
                var myListStr = new List<string>();
                myListStr.Add("ws" + i);
                myListStr.Add("second" + i);
                jg.jaggedListStr.Add(myListStr);

                var simple = new SimpleClass();
                simple.Name = "ssss";
                jg.complexJaggedList = new List<List<SimpleClass>>();
                var listSimple = new List<SimpleClass>();
                listSimple.Add(simple);

                jg.complexJaggedList.Add(listSimple);
                jg.complexJaggedList.Add(listSimple);
                s_db.StoreObject(jg);
            }

            IList<JaggedTy> all = s_db.LoadAll<JaggedTy>();
            for (var i = 0; i < 10; i++) s_db.Delete(all[i]);
            s_db.Close();

            SiaqodbUtil.Shrink(objPath, ShrinkType.Normal);
            SiaqodbUtil.Shrink(objPath, ShrinkType.ForceClaimSpace);

            s_db = new Siaqodb(objPath);
            all = s_db.LoadAll<JaggedTy>();

            for (var i = 10; i < 20; i++)
            {
                var j = i - 10;
                Assert.AreEqual((byte)i, all[j].jaggedByte[0][0]);
                Assert.AreEqual((byte)(i + 1), all[j].jaggedByte[0][1]);
                Assert.AreEqual((byte)(i + 2), all[j].jaggedByte[1][0]);
                Assert.AreEqual((byte)(i + 3), all[j].jaggedByte[1][1]);

                Assert.AreEqual(i, all[j].jaggedInt[0][0]);
                Assert.AreEqual(i + 1, all[j].jaggedInt[0][1]);
                Assert.AreEqual(i + 2, all[j].jaggedInt[1][0]);
                Assert.AreEqual(i + 3, all[j].jaggedInt[1][1]);

                Assert.AreEqual(2, all[j].jaggedList[0].Count);
                Assert.AreEqual(2, all[j].jaggedList.Count);
                Assert.AreEqual(i, all[j].jaggedList[0][0]);
                Assert.AreEqual(i + 1, all[j].jaggedList[0][1]);
                Assert.AreEqual(i * 10, all[j].jaggedList[1][0]);
                Assert.AreEqual(i * 100, all[j].jaggedList[1][1]);

                Assert.AreEqual(1, all[j].jaggedListStr.Count);
                Assert.AreEqual(2, all[j].jaggedListStr[0].Count);
                Assert.AreEqual("ws" + i, all[j].jaggedListStr[0][0]);
                Assert.AreEqual("second" + i, all[j].jaggedListStr[0][1]);
                Assert.AreEqual(2, all[j].complexJaggedList.Count);
                Assert.IsNotNull(all[j].complexJaggedList[0][0]);
            }

            var q = from JaggedTy jgd in s_db
                where jgd.jaggedInt.Length == 2
                select jgd;

            Assert.AreEqual(10, q.ToList().Count);

            var myInList = new List<int>();
            myInList.Add(10);
            myInList.Add(11);

            var q2 = from JaggedTy jgd in s_db
                where jgd.jaggedList.Contains(myInList)
                select jgd;

            Assert.AreEqual(1, q2.ToList().Count);
        }

        [Test]
        public void TestStoreNMatrix()
        {
            var s_db = new Siaqodb(objPath);
            s_db.DropType<NMatrixTy>();
            for (var i = 0; i < 10; i++)
            {
                var jg = new NMatrixTy();
                jg.matrix3Int = new int[2][][];
                jg.matrix3Int[0] = new int[2][];
                jg.matrix3Int[0][0] = new int[2];
                jg.matrix3Int[0][0][0] = i;
                jg.matrix3Int[0][0][1] = i + 1;
                jg.matrix3Int[0][1] = new int[2];
                jg.matrix3Int[0][1][0] = i + 2;
                jg.matrix3Int[0][1][1] = i + 3;

                jg.matrix3Int[1] = new int[2][];
                jg.matrix3Int[1][0] = new int[2];
                jg.matrix3Int[1][0][0] = i;

                jg.matrixStr = new List<List<List<string>>>();
                var jaggedList = new List<List<string>>();
                var myListStr = new List<string>();
                myListStr.Add("ws" + i);
                myListStr.Add("second" + i);
                jaggedList.Add(myListStr);
                jaggedList.Add(myListStr);
                jg.matrixStr.Add(jaggedList);
                jg.matrixStr.Add(jaggedList);

                jg.listArrayMatrix = new List<int[][]>();
                var arr = new int[2][];
                arr[0] = new int[2];
                arr[0][0] = i;
                arr[0][1] = i + 1;

                jg.listArrayMatrix.Add(arr);
                jg.listArrayMatrix.Add(arr);

                s_db.StoreObject(jg);
            }

            IList<NMatrixTy> all = s_db.LoadAll<NMatrixTy>();
            Assert.AreEqual(10, all.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(i, all[i].matrix3Int[0][0][0]);
                Assert.AreEqual(i + 1, all[i].matrix3Int[0][0][1]);
                Assert.AreEqual(2, all[i].matrix3Int[0].Length);
                Assert.AreEqual(2, all[i].matrix3Int[0][1].Length);


                Assert.AreEqual(2, all[i].matrixStr.Count);
                Assert.AreEqual(2, all[i].matrixStr[0].Count);
                Assert.AreEqual(2, all[i].matrixStr[1].Count);
                Assert.AreEqual("ws" + i, all[i].matrixStr[0][0][0]);
                Assert.AreEqual("second" + i, all[i].matrixStr[0][0][1]);
                Assert.AreEqual("ws" + i, all[i].matrixStr[0][1][0]);
                Assert.AreEqual("second" + i, all[i].matrixStr[0][1][1]);
                Assert.AreEqual("ws" + i, all[i].matrixStr[1][1][0]);
                Assert.AreEqual("second" + i, all[i].matrixStr[1][1][1]);

                Assert.AreEqual(i, all[i].listArrayMatrix[0][0][0]);
                Assert.AreEqual(i + 1, all[i].listArrayMatrix[1][0][1]);
            }
        }

        [Test]
        public void TestStoreDictionaryAndShrink()
        {
            var s_db = new Siaqodb(objPath);

            s_db.DropType<WithDict>();
            s_db.DropType<JaggedTy>();
            s_db.DropType<NMatrixTy>();
            for (var i = 0; i < 20; i++)
            {
                var dict = new WithDict();
                dict.DictInt = new Dictionary<int, int>();
                dict.DictStr = new Dictionary<byte, string>();
                dict.DictComplex = new Dictionary<JaggedTy, int>();
                dict.ZuperDict = new Dictionary<uint, NMatrixTy>();

                for (var j = 0; j < 5; j++)
                {
                    dict.DictInt[j] = i + j;
                    dict.ZuperDict[(uint)j] = new NMatrixTy();
                    dict.DictStr[(byte)j] = "sss" + i;
                    var jt = new JaggedTy();
                    dict.DictComplex[jt] = j + i;
                }

                s_db.StoreObject(dict);
            }

            IList<WithDict> all = s_db.LoadAll<WithDict>();
            for (var i = 0; i < 10; i++) s_db.Delete(all[i]);
            s_db.Close();

            SiaqodbUtil.Shrink(objPath, ShrinkType.Normal);
            SiaqodbUtil.Shrink(objPath, ShrinkType.ForceClaimSpace);

            s_db = new Siaqodb(objPath);
            all = s_db.LoadAll<WithDict>();

            for (var i = 10; i < 20; i++)
            {
                var j = i - 10;
                Assert.AreEqual(5, all[j].DictInt.Keys.Count);
                Assert.AreEqual(5, all[j].DictStr.Keys.Count);
                Assert.AreEqual(5, all[j].DictComplex.Keys.Count);
                Assert.AreEqual(5, all[j].ZuperDict.Keys.Count);
                for (var k = 0; k < 5; k++)
                {
                    Assert.AreEqual(i + k, all[j].DictInt[k]);
                    Assert.AreEqual("sss" + i, all[j].DictStr[(byte)k]);
                    Assert.IsNotNull(all[j].ZuperDict[(uint)k]);
                }
            }


            var q = from WithDict d in s_db
                where d.DictInt.ContainsKey(1)
                select d;
            Assert.AreEqual(10, q.ToList().Count);
            q = from WithDict d in s_db
                where d.DictInt.ContainsValue(11)
                select d;
            Assert.AreEqual(2, q.ToList().Count);

            q = from WithDict d in s_db
                where d.DictInt.ContainsKey(-1)
                select d;
            Assert.AreEqual(0, q.ToList().Count);
        }

        [Test]
        public void TestStoreDictionary()
        {
            var s_db = new Siaqodb(objPath);

            s_db.DropType<WithDict>();
            s_db.DropType<JaggedTy>();
            s_db.DropType<NMatrixTy>();
            for (var i = 0; i < 10; i++)
            {
                var dict = new WithDict();
                dict.DictInt = new Dictionary<int, int>();
                dict.DictStr = new Dictionary<byte, string>();
                dict.DictComplex = new Dictionary<JaggedTy, int>();
                dict.ZuperDict = new Dictionary<uint, NMatrixTy>();

                for (var j = 0; j < 5; j++)
                {
                    dict.DictInt[j] = i + j;
                    dict.ZuperDict[(uint)j] = new NMatrixTy();
                    dict.DictStr[(byte)j] = "sss" + i;
                    var jt = new JaggedTy();
                    dict.DictComplex[jt] = j + i;
                }

                s_db.StoreObject(dict);
            }

            IList<WithDict> all = s_db.LoadAll<WithDict>();
            Assert.AreEqual(10, all.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(5, all[i].DictInt.Keys.Count);
                Assert.AreEqual(5, all[i].DictStr.Keys.Count);
                Assert.AreEqual(5, all[i].DictComplex.Keys.Count);
                Assert.AreEqual(5, all[i].ZuperDict.Keys.Count);
                for (var j = 0; j < 5; j++)
                {
                    Assert.AreEqual(i + j, all[i].DictInt[j]);
                    Assert.AreEqual("sss" + i, all[i].DictStr[(byte)j]);
                    Assert.IsNotNull(all[i].ZuperDict[(uint)j]);
                }
            }

            s_db.Close();
            s_db = new Siaqodb(objPath);
            all = s_db.LoadAll<WithDict>();
            Assert.AreEqual(10, all.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(5, all[i].DictInt.Keys.Count);
                Assert.AreEqual(5, all[i].DictStr.Keys.Count);
                Assert.AreEqual(5, all[i].DictComplex.Keys.Count);
                Assert.AreEqual(5, all[i].ZuperDict.Keys.Count);
                for (var j = 0; j < 5; j++)
                {
                    Assert.AreEqual(i + j, all[i].DictInt[j]);
                    Assert.AreEqual("sss" + i, all[i].DictStr[(byte)j]);
                    Assert.IsTrue(all[i].ZuperDict[(uint)j].OID > 0);
                }
            }

            var q = from WithDict d in s_db
                where d.DictInt.ContainsKey(1)
                select d;
            Assert.AreEqual(10, q.ToList().Count);
            q = from WithDict d in s_db
                where d.DictInt.ContainsValue(1)
                select d;
            Assert.AreEqual(2, q.ToList().Count);

            q = from WithDict d in s_db
                where d.DictInt.ContainsKey(-1)
                select d;
            Assert.AreEqual(0, q.ToList().Count);
        }

        [Test]
        public void TestUpdateDictionary()
        {
            var s_db = new Siaqodb(objPath);

            s_db.DropType<WithDict>();
            s_db.DropType<JaggedTy>();
            s_db.DropType<NMatrixTy>();
            for (var i = 0; i < 10; i++)
            {
                var dict = new WithDict();
                dict.DictInt = new Dictionary<int, int>();
                dict.DictStr = new Dictionary<byte, string>();
                dict.DictComplex = new Dictionary<JaggedTy, int>();
                dict.ZuperDict = new Dictionary<uint, NMatrixTy>();

                for (var j = 0; j < 5; j++)
                {
                    dict.DictInt[j] = i + j;
                    dict.ZuperDict[(uint)j] = new NMatrixTy();
                    dict.DictStr[(byte)j] = "sss" + i;
                    var jt = new JaggedTy();
                    dict.DictComplex[jt] = j + i;
                }

                s_db.StoreObject(dict);
            }

            IList<WithDict> all = s_db.LoadAll<WithDict>();
            for (var i = 0; i < 10; i++)
            {
                for (var j = 0; j < 5; j++)
                {
                    all[i].DictInt[j] = i + j + 10;
                    all[i].DictStr[(byte)j] = "updated test";
                    all[i].ZuperDict[(uint)j] = new NMatrixTy();
                }

                s_db.StoreObject(all[i]);
            }

            all = s_db.LoadAll<WithDict>();

            Assert.AreEqual(10, all.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(5, all[i].DictInt.Keys.Count);
                Assert.AreEqual(5, all[i].DictStr.Keys.Count);
                Assert.AreEqual(5, all[i].DictComplex.Keys.Count);
                Assert.AreEqual(5, all[i].ZuperDict.Keys.Count);
                for (var j = 0; j < 5; j++)
                {
                    Assert.AreEqual(i + j + 10, all[i].DictInt[j]);
                    Assert.AreEqual("updated test", all[i].DictStr[(byte)j]);
                    Assert.IsNotNull(all[i].ZuperDict[(uint)j]);
                }
            }

            s_db.Close();
            s_db = new Siaqodb(objPath);
            all = s_db.LoadAll<WithDict>();
            Assert.AreEqual(10, all.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(5, all[i].DictInt.Keys.Count);
                Assert.AreEqual(5, all[i].DictStr.Keys.Count);
                Assert.AreEqual(5, all[i].DictComplex.Keys.Count);
                Assert.AreEqual(5, all[i].ZuperDict.Keys.Count);
                for (var j = 0; j < 5; j++)
                {
                    Assert.AreEqual(i + j + 10, all[i].DictInt[j]);
                    Assert.AreEqual("updated test", all[i].DictStr[(byte)j]);
                    Assert.IsNotNull(all[i].ZuperDict[(uint)j]);
                }
            }

            var q = from WithDict d in s_db
                where d.DictInt.ContainsKey(1)
                select d;
            Assert.AreEqual(10, q.ToList().Count);
            q = from WithDict d in s_db
                where d.DictInt.ContainsValue(11)
                select d;
            Assert.AreEqual(2, q.ToList().Count);

            q = from WithDict d in s_db
                where d.DictInt.ContainsKey(-1)
                select d;
            Assert.AreEqual(0, q.ToList().Count);
        }

        [Test]
        public void TestStoreDictionaryTransactional()
        {
            var s_db = new Siaqodb(objPath);


            s_db.DropType<WithDict>();
            s_db.DropType<JaggedTy>();
            s_db.DropType<NMatrixTy>();
            var transaction = s_db.BeginTransaction();
            for (var i = 0; i < 10; i++)
            {
                var dict = new WithDict();
                dict.DictInt = new Dictionary<int, int>();
                dict.DictStr = new Dictionary<byte, string>();
                dict.DictComplex = new Dictionary<JaggedTy, int>();
                dict.ZuperDict = new Dictionary<uint, NMatrixTy>();

                for (var j = 0; j < 5; j++)
                {
                    dict.DictInt[j] = i + j;
                    dict.ZuperDict[(uint)j] = new NMatrixTy();
                    dict.DictStr[(byte)j] = "sss" + i;
                    var jt = new JaggedTy();
                    dict.DictComplex[jt] = j + i;
                }

                s_db.StoreObject(dict, transaction);
            }

            transaction.Commit();
            try
            {
                transaction = s_db.BeginTransaction();
                for (var i = 0; i < 10; i++)
                {
                    var dict = new WithDict();
                    dict.DictInt = new Dictionary<int, int>();
                    dict.DictStr = new Dictionary<byte, string>();
                    dict.DictComplex = new Dictionary<JaggedTy, int>();
                    dict.ZuperDict = new Dictionary<uint, NMatrixTy>();

                    for (var j = 0; j < 5; j++)
                    {
                        dict.DictInt[j] = i + j;
                        dict.ZuperDict[(uint)j] = new NMatrixTy();
                        dict.DictStr[(byte)j] = "sss" + i;
                        var jt = new JaggedTy();
                        dict.DictComplex[jt] = j + i;
                    }

                    s_db.StoreObject(dict, transaction);
                    if (i == 5) throw new Exception("need some rollback");
                }

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                if (ex.Message == "need some rollback")
                {
                }
                else
                {
                    throw ex;
                }
            }

            IList<WithDict> all = s_db.LoadAll<WithDict>();
            Assert.AreEqual(10, all.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(5, all[i].DictInt.Keys.Count);
                Assert.AreEqual(5, all[i].DictStr.Keys.Count);
                Assert.AreEqual(5, all[i].DictComplex.Keys.Count);
                Assert.AreEqual(5, all[i].ZuperDict.Keys.Count);
                for (var j = 0; j < 5; j++)
                {
                    Assert.AreEqual(i + j, all[i].DictInt[j]);
                    Assert.AreEqual("sss" + i, all[i].DictStr[(byte)j]);
                    Assert.IsNotNull(all[i].ZuperDict[(uint)j]);
                }
            }

            s_db.Close();
            s_db = new Siaqodb(objPath);
            all = s_db.LoadAll<WithDict>();
            Assert.AreEqual(10, all.Count);
            for (var i = 0; i < 10; i++)
            {
                Assert.AreEqual(5, all[i].DictInt.Keys.Count);
                Assert.AreEqual(5, all[i].DictStr.Keys.Count);
                Assert.AreEqual(5, all[i].DictComplex.Keys.Count);
                Assert.AreEqual(5, all[i].ZuperDict.Keys.Count);
                for (var j = 0; j < 5; j++)
                {
                    Assert.AreEqual(i + j, all[i].DictInt[j]);
                    Assert.AreEqual("sss" + i, all[i].DictStr[(byte)j]);
                    Assert.IsTrue(all[i].ZuperDict[(uint)j].OID > 0);
                }
            }

            var q = from WithDict d in s_db
                where d.DictInt.ContainsKey(1)
                select d;
            Assert.AreEqual(10, q.ToList().Count);
            q = from WithDict d in s_db
                where d.DictInt.ContainsValue(1)
                select d;
            Assert.AreEqual(2, q.ToList().Count);

            q = from WithDict d in s_db
                where d.DictInt.ContainsKey(-1)
                select d;
            Assert.AreEqual(0, q.ToList().Count);
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

    public class JaggedTy
    {
        public List<List<SimpleClass>> complexJaggedList;
        public int HH;
        public byte[][] jaggedByte;
        public byte[][] jaggedByte1;
        public int[][] jaggedInt;
        public List<List<int>> jaggedList;
        public List<List<int>> jaggedList1;
        public List<List<string>> jaggedListStr;
        public List<List<string>> jaggedListStr1;
        public int OID { get; set; }
    }

    public class SimpleClass
    {
        public int OID { get; set; }
        public string Name { get; set; }
    }

    public class NMatrixTy
    {
        public List<int[][]> listArrayMatrix;
        public int[][][] matrix3Int;
        public List<List<List<string>>> matrixStr;
        public List<List<List<string>>> matrixStr1;
        public int OID { get; set; }
    }

    public class WithDict
    {
        public Dictionary<JaggedTy, int> DictComplex;
        public Dictionary<JaggedTy, int> DictComplex22;
        public Dictionary<int, int> DictInt;
        public Dictionary<byte, string> DictStr;
        private int GG;

        public int[][] jaggedInt;
        public int[][][] jaggedInt3;
        public Dictionary<uint, NMatrixTy> ZuperDict;
        public int OID { get; set; }
    }
}