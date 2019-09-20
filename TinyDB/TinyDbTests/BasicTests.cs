using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
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
                var info3 = subject.Store("sub/test3", ms3);


                var ms_result = new MemoryStream();
                var info_result = subject.Read(info2.ID, ms_result);

                ms_result.Seek(0, SeekOrigin.Begin);
                var result = Encoding.UTF8.GetString(ms_result.ToArray());

                Assert.That(result, Is.EqualTo(message), "message changed");
                Assert.That(info_result.ID, Is.EqualTo(info2.ID), "Id changed");

                Assert.That(info_result.ID, Is.Not.EqualTo(info1.ID), "Id mismatch");
                Assert.That(info_result.ID, Is.Not.EqualTo(info3.ID), "Id mismatch");


                var list = string.Join(", ", subject.ListFiles().Select(e=>e.FileName));
                Assert.That(list, Is.EqualTo("test1, test2, sub/test3"));
            }
        }

        
        [Test]
        public void can_get_files_by_complete_path () {

            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms1 = MakeSourceStream("This is my file content");

                subject.Store("find/me/test1", ms1);

                string recovered;

                using (var ms2 = new MemoryStream())
                {

                    var result = subject.Read("find/me/test1", ms2);

                    Assert.That(result, Is.Not.Null, "Null entry returned");
                    Assert.That(result.FileName, Is.EqualTo("find/me/test1"));

                    ms2.Seek(0, SeekOrigin.Begin);
                    recovered = Encoding.UTF8.GetString(ms2.ToArray());
                }

                Assert.That(recovered, Is.EqualTo("This is my file content"));
            }
        }

        [Test]
        public void can_find_files_by_partial_path () {

            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms1 = MakeSourceStream();
                var ms2 = MakeSourceStream();

                subject.Store("find/me/test1", ms1);
                subject.Store("find/me/test2", ms2);
                //subject.Store("find/me/sub/path/test2.5", ms25);
                subject.Store("not/me/test3", MakeSourceStream());
                subject.Store("find/not/me/test4", MakeSourceStream());

                var result = subject.FindFiles("find/me");
                Assert.That(result, Is.Not.Null, "Null array returned");
                
                var list = string.Join(", ", result.Select(e=>e.FileName));
                Assert.That(list, Is.EqualTo("find/me/test1, find/me/test2"));
            }
        }

        [Test]
        public void can_connect_to_a_datastore_using_a_seekable_stream () {
            byte[] raw;
            EntryInfo info;
            // test creating a new store in an empty stream
            using (var stream = new MemoryStream())
            {
                var subject = Datastore.TryConnect(stream);
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var ms = MakeSourceStream();

                info = subject.Store("test_name", ms);

                Assert.That(info, Is.Not.Null, "Storage result was invalid");

                var ms2 = new MemoryStream();
                var info2 = subject.Read(info.ID, ms2);
                ms2.Seek(0, SeekOrigin.Begin);

                Console.WriteLine($"{info.ID} -> {info2.ID}");

                var result = Encoding.UTF8.GetString(ms2.ToArray());
                Assert.That(result, Is.EqualTo(message), "message changed");
                Assert.That(info.ID, Is.EqualTo(info2.ID), "Id changed");

                Assert.That(stream.Length, Is.GreaterThan(0), "Stream was not used");

                subject.Flush();
                stream.Seek(0, SeekOrigin.Begin);
                raw = stream.ToArray(); // copy results
                subject.Dispose();
            }

            // test reading from an existing stream
            using (var stream = new MemoryStream(raw))
            {
                stream.Seek(0, SeekOrigin.Begin);
                var subject = Datastore.TryConnect(stream);
                var ms2 = new MemoryStream();
                var info2 = subject.Read(info.ID, ms2); // IDs don't persist?
                ms2.Seek(0, SeekOrigin.Begin);

                Console.WriteLine($"{info.ID} -> {info2.ID} -- Found ['{info2.FileName}'] in the captured stream, {info2.FileLength} bytes.");

                var result = Encoding.UTF8.GetString(ms2.ToArray());
                Assert.That(result, Is.EqualTo(message), "message changed");
                Assert.That(info.ID, Is.EqualTo(info2.ID), "Id changed");
            }
        }
        
        [Test]
        public void writing_to_an_existing_file_overwrites_it () {
            // TODO: this currently gives us two files with the same name.
            // it should result in the equivalent of a delete+store
            
            using (var subject = Datastore.TryConnect(TestFilePath)) {
                Assert.That(subject, Is.Not.Null, "Database connector failed");

                var firstFile = MakeSourceStream("I am first file");
                var secondFile = MakeSourceStream("file two");

                var info_A = subject.Store("test_name", firstFile);
                var info_B = subject.Store("test_name", secondFile);

                Assert.That(info_A, Is.Not.Null, "Storage result was invalid");
                Assert.That(info_B, Is.Not.Null, "Storage result was invalid");
                
                var storedFiles = subject.ListFiles();
                var list = string.Join(", ", storedFiles.Select(e=>e.FileName + " = " + e.ID.ShortString()));
                Assert.That(storedFiles.Length, Is.EqualTo(1), $"Expected one file, but got {storedFiles.Length} --> {list}");

                Console.WriteLine($"{info_A.ID} then {info_B.ID}");

                var ms2 = new MemoryStream();
                var info2 = subject.Read(info_B.ID, ms2);
                ms2.Seek(0, SeekOrigin.Begin);
                
                Console.WriteLine($"{info_B.ID} -> {info2.ID}");

                var result = Encoding.UTF8.GetString(ms2.ToArray());
                Assert.That(result, Is.EqualTo("file two"), "unexpected message");
                Assert.That(info_B.ID, Is.EqualTo(info2.ID), "Id changed");
            }
        }

        [Test]
        public void a_crash_halfway_through_a_write_is_recoverable () {
            Assert.Fail("Not yet implemented");
        }

        [Test]
        public void multiple_threads_reading_and_writing_is_supported () {
            ThreadPool.SetMinThreads(10,10);
            ThreadPool.SetMaxThreads(10,10);

            int count = 0;
            const int target = 100;

            using (var stream = new MemoryStream())
            {
                var subject = Datastore.TryConnect(stream);

                for (int i = 0; i < target; i++)
                {
                    var j = i;// unbind closure
                    ThreadPool.QueueUserWorkItem(x =>
                    {
                        try
                        {
                            var data = MakeSourceStream("Data for item " + j);
                            var info = subject.Store($"file_{j}", data);
                            
                            ThreadPool.QueueUserWorkItem(y => {
                                using (var result = new MemoryStream())
                                {
                                    var entry = subject.Read(info.ID, result);
                                    result.Seek(0, SeekOrigin.Begin);
                                    var str = Encoding.UTF8.GetString(result.ToArray());
                                    Assert.That(entry.FileName, Is.EqualTo($"file_{j}"));
                                    Assert.That(str, Is.EqualTo("Data for item " + j));
                                }
                            });
                        }
                        catch (Exception ex) {
                            Console.WriteLine($"Writing item {j} failed with: " + ex);
                        }
                        finally
                        {
                            Interlocked.Increment(ref count);
                        }
                    });
                }

                // wait for threads
                while (count < target) { Thread.Sleep(100); }

                var everything = subject.ListFiles();
                Assert.That(everything.Length, Is.EqualTo(target), $"Expected {target} files, but got {everything.Length}");
            }
        }

        [Test]
        public void multiple_threads_writing_is_supported () {
            ThreadPool.SetMinThreads(10,10);
            ThreadPool.SetMaxThreads(10,10);

            int count = 0;
            const int target = 100;

            using (var stream = new MemoryStream())
            {
                var subject = Datastore.TryConnect(stream);

                for (int i = 0; i < target; i++)
                {
                    var j = i;// unbind closure
                    ThreadPool.QueueUserWorkItem(x =>
                    {
                        try
                        {
                            var data = MakeSourceStream("Data for item " + j);
                            subject.Store($"file_{j}", data);
                            //Console.Write($"; {j} ok .");
                        }
                        catch (Exception ex) {
                            Console.WriteLine($"Item {j} failed with: " + ex);
                        }
                        finally
                        {
                            Interlocked.Increment(ref count);
                        }
                    });
                }

                // wait for threads
                while (count < target) { Thread.Sleep(100); }

                var everything = subject.ListFiles();
                Assert.That(everything.Length, Is.EqualTo(target), $"Expected {target} files, but got {everything.Length}");
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
        
        private static MemoryStream MakeSourceStream(string msg)
        {
            var ms = new MemoryStream();
            var buf = Encoding.UTF8.GetBytes(msg);
            ms.Write(buf, 0, buf.Length);
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

    }

}
