using System;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;
using TinyDB;
// ReSharper disable PossibleNullReferenceException
// ReSharper disable AssignNullToNotNullAttribute

namespace TinyDbTests
{
    [TestFixture]
    public class BasicTests
    {
        const string TestFilePath = @"C:\Temp\TinyDB\testfile.db";
        const string message = "Hello, world. This is my binary test";

        [SetUp]
        public void Cleanup()
        {
            if (File.Exists(TestFilePath)) File.Delete(TestFilePath);
        }

        [Test]
        public void can_connect_to_a_db_file ()
        {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");
            }
        }

        [Test]
        public void can_store_and_recover_a_file ()
        {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms = MakeSourceStream();

                var info = subject.Store("test_name", ms);

                Assert.That(info, Is.Not.Null, "Storage result was invalid");

                var ms2 = new MemoryStream();
                var info2 = subject.Read(info.ID, ms2);
                ms2.Seek(0, SeekOrigin.Begin);
                
                Console.WriteLine($"{info.ID} -> {info2.ID}");

                var result = Encoding.UTF8.GetString(ms2.ToArray());
                Assert.That(result, Is.EqualTo(message), "message changed");
                Assert.That(info.ID, Is.EqualTo(info2.ID), "Id changed");
            }
        }
        
        [Test]
        public void supports_long_file_names ()
        {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms = MakeSourceStream();

                var info = subject.Store("this/is/my/relatively-long/characteristic+$JSON.stringify/file.name", ms);

                Assert.That(info, Is.Not.Null, "Storage result was invalid");

                var ms2 = new MemoryStream();
                var info2 = subject.Read(info.ID, ms2);
                ms2.Seek(0, SeekOrigin.Begin);
                
                Console.WriteLine($"{info.ID} -> {info2.ID}");
                Console.WriteLine($"{info.FileName} -> {info2.FileName}");

                var result = Encoding.UTF8.GetString(ms2.ToArray());
                Assert.That(result, Is.EqualTo(message), "message changed");
                Assert.That(info.ID, Is.EqualTo(info2.ID), "Id changed");
            }
        }


        [Test]
        public void can_read_file_info_with_null_stream ()
        {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms = MakeSourceStream();

                var info = subject.Store("test_name", ms);

                Assert.That(info, Is.Not.Null, "Storage result was invalid");

                var info2 = subject.Read(info.ID, null);
                
                Console.WriteLine($"{info.ID} -> {info2.ID}");
                Console.WriteLine($"{info.FileName} -> {info2.FileName}");

                Assert.That(info.ID, Is.EqualTo(info2.ID), "Id changed");
                Assert.That(info.FileName, Is.EqualTo(info2.FileName), "Id changed");
            }
        }
        
        [Test]
        public void deleting_a_file_makes_it_unavailable ()
        {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms = MakeSourceStream();

                var info = subject.Store("test_name", ms);

                Assert.That(info, Is.Not.Null, "Storage result was invalid");

                var info2 = subject.Read(info.ID, null);
                Assert.That(info2, Is.Not.Null, "File did not store");

                var ok = subject.Delete(info2.ID);
                var info3 = subject.Read(info.ID, null);
                Assert.That(ok, Is.True, "delete failed");
                Assert.That(info3, Is.Null, "query succeeded, but should have failed");
            }
        }

        [Test]
        public void can_store_multiple_files () {
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms1 = MakeSourceStream();
                var ms2 = MakeSourceStream();
                var ms3 = MakeSourceStream();

                var info1 = subject.Store("test1", ms1);
                var info2 = subject.Store("test2", ms2);
                var info3 = subject.Store("test3", ms3);


                var ms_result = new MemoryStream();
                var info_result = subject.Read(info2.ID, ms_result);

                ms_result.Seek(0, SeekOrigin.Begin);
                var result = Encoding.UTF8.GetString(ms_result.ToArray());

                Assert.That(result, Is.EqualTo(message), "message changed");
                Assert.That(info_result.ID, Is.EqualTo(info2.ID), "Id changed");

                Assert.That(info_result.ID, Is.Not.EqualTo(info1.ID), "Id mismatch");
                Assert.That(info_result.ID, Is.Not.EqualTo(info3.ID), "Id mismatch");


                var list = string.Join(", ", subject.ListFiles().Select(e=>e.FileName));
                Assert.That(list, Is.EqualTo("test1, test2, test3"));
            }
        }

        private static MemoryStream MakeSourceStream()
        {
            var ms = new MemoryStream();
            var buf = Encoding.UTF8.GetBytes(message);
            ms.Write(buf, 0, buf.Length);
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

    }

}
