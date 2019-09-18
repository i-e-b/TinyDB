using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
namespace JetBrains.Annotations {
    /// <summary>Marked element could be <c>null</c></summary>
    [AttributeUsage(AttributeTargets.All)] internal sealed class CanBeNullAttribute : Attribute { }
    /// <summary>Marked element could never be <c>null</c></summary>
    [AttributeUsage(AttributeTargets.All)] internal sealed class NotNullAttribute : Attribute { }
    /// <summary>IEnumerable, Task.Result, or Lazy.Value property can never be null.</summary>
    [AttributeUsage(AttributeTargets.All)] internal sealed class ItemNotNullAttribute : Attribute { }
    /// <summary>IEnumerable, Task.Result, or Lazy.Value property can be null.</summary>
    [AttributeUsage(AttributeTargets.All)] internal sealed class ItemCanBeNullAttribute : Attribute { }
}

namespace TinyDB
{
    using JetBrains.Annotations;

    /// <summary>
    /// This is the entry point to the data storage
    /// </summary>
    public class Datastore : IDisposable
    {
        [NotNull] private readonly Stream _fs;
        [NotNull] private readonly Engine _engine;

        private Datastore(Stream fs, Engine engine)
        {
            _fs = fs ?? throw new ArgumentNullException(nameof(fs));
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        }


        /// <summary>
        /// Open a connection to a datastore by file path
        /// </summary>
        public static Datastore TryConnect(string storagePath){
            if (string.IsNullOrWhiteSpace(storagePath)) throw new ArgumentNullException(nameof(storagePath));

            var normalPath = Path.GetFullPath(storagePath);
            var directory = Path.GetDirectoryName(normalPath) ?? "";

            if (!string.IsNullOrWhiteSpace(directory)) Directory.CreateDirectory(directory);
            if (!File.Exists(normalPath))
            {
                using (var fileStream = new FileStream(normalPath, FileMode.CreateNew, FileAccess.Write))
                {
                    Engine.CreateEmptyFile(fileStream);
                }
            }

            var fs = new FileStream(normalPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, (int)BasePage.PAGE_SIZE, FileOptions.None);
            var engine = new Engine(fs);

            return new Datastore(fs, engine);
        }
        
        /// <summary>
        /// Open a connection to a datastore by seekable stream.
        /// Throws an exception if the stream does not support seeking and reading.
        /// <para></para>
        /// If an empty stream is provided (length == 0), it will be initialised. Otherwise it must be
        /// a valid storage stream.
        /// </summary>
        public static Datastore TryConnect(Stream storage)
        {
            if (storage == null || !storage.CanSeek || !storage.CanRead) throw new ArgumentException("Storage stream must support seeking and reading", nameof(storage));

            if (storage.Length == 0)
            {
                if (!storage.CanWrite) throw new ArgumentException("Attempted to initialise a read-only stream", nameof(storage));
                Engine.CreateEmptyFile(storage);
                storage.Seek(0, SeekOrigin.Begin);
            }
            var engine = new Engine(storage);

            return new Datastore(storage, engine);
        }


        /// <summary>
        /// Write a file to the given path, returning the entry info
        /// </summary>
        /// <param name="fileName">File name and path</param>
        /// <param name="input">Readable stream. Don't forget to seek.</param>
        /// <returns>File entry info, including storage key</returns>
        public EntryInfo Store([NotNull]string fileName, [NotNull]Stream input)
        {
            if (input == null) throw new ArgumentNullException(nameof(input));

            lock (_engine)
            {
                var entry = new EntryInfo(fileName);
                _engine.Write(entry, input);
                return entry;
            }
        }

        /// <summary>
        /// Read stored file to a stream, also returning file details.
        /// If a null stream is passed, only details will be returned.
        /// Returns null if no file was found.
        /// </summary>
        /// <param name="id">File ID</param>
        /// <param name="output">Writable stream. Don't forget to seek before calling.</param>
        /// <returns>File entry info</returns>
        public EntryInfo Read(Guid id, Stream output)
        {
            if (output == null)
            {
                var indexNode = _engine.Search(id);
                return indexNode == null ? null : new EntryInfo(indexNode);
            }

            return _engine.Read(id, output);
        }

        /// <summary>
        /// Delete a file by ID.
        /// Ignores requests to delete non existent files
        /// </summary>
        /// <param name="id">File ID</param>
        /// <returns>Returns true if there wa a file to be deleted</returns>
        public bool Delete(Guid id)
        {
            return _engine.Delete(id);
        }

        /// <summary>
        /// List all files currently stored
        /// </summary>
        public EntryInfo[] ListFiles(){
            return _engine.ListAllFiles();
        }

        /// <summary>
        /// Save changes and close file
        /// </summary>
        public void Dispose()
        {
            _engine.PersistPages(); // Write any pages still cached in memory

            if (_fs.CanWrite) _fs.Flush();

            _engine.Dispose();
            _fs.Dispose();
        }

        /// <summary>
        /// Find all files under a path root
        /// </summary>
        /// <param name="pathRoot">Left-side of a path to match</param>
        /// <returns>Found file entries</returns>
        public EntryInfo[] FindFiles(string pathRoot)
        {
            // TODO: the plan is to have pages of path-Trie linked to file IDs
            return null;
        }

        /// <summary>
        /// Write any cached data to the underlying storage
        /// </summary>
        public void Flush()
        {
            _engine.PersistPages();
            if (_fs.CanWrite) _fs.Flush();
        }
    }

    /// <summary>
    /// Provides thread-locked access to a stream, as either a BinaryReader or BinaryWriter
    /// </summary>
    public class ThreadlockBinaryStream : IDisposable
    {
        private volatile Stream _token, _master;
        private readonly bool _closeBase;
        [NotNull]private readonly object _lock = new object();

        public ThreadlockBinaryStream([NotNull]Stream baseStream, bool closeBaseStream = true)
        {
            _token = baseStream;
            _master = baseStream;
            _closeBase = closeBaseStream;

            if (baseStream is FileStream fileStream) fileStream.TryLockFile(0, fileStream.Length, 5);
        }

        /// <summary>
        /// Wait for access to the reader.
        /// *MUST* always be released correctly
        /// </summary>
        [NotNull]
        public BinaryReader AcquireReader() {
            lock (_lock)
            {
                var stream = Interlocked.Exchange(ref _token, null);
                while (stream == null)
                {
                    Thread.Sleep(0);
                    stream = Interlocked.Exchange(ref _token, null);
                }
                return new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
            }
        }

        /// <summary>
        /// Wait for access to the writer.
        /// *MUST* always be released correctly
        /// </summary>
        [NotNull]
        public BinaryWriter AcquireWriter() {
            lock (_lock)
            {
                var stream = Interlocked.Exchange(ref _token, null);
                while (stream == null)
                {
                    Thread.Sleep(0);
                    stream = Interlocked.Exchange(ref _token, null);
                }
                return new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
            }
        }

        public void Release(BinaryReader reader) {
            lock (_lock)
            {
                if (reader == null || reader.BaseStream != _master) throw new Exception("Invalid threadlock stream release (reader)");
                reader.Dispose();
                _token = _master;
            }
        }

        public void Release(BinaryWriter writer) {
            lock (_lock)
            {
                if (writer == null || writer.BaseStream != _master) throw new Exception("Invalid threadlock stream release (writer)");
                writer.Flush();
                writer.Dispose();
                _token = _master;
            }
        }

        public void Close()
        {
            _token = null;
            if (_master is FileStream fileStream)
            {
                try
                {
                    fileStream.Unlock(0, fileStream.Length);
                }
                catch
                {
                    // Ignore
                }
            }
            if (_closeBase) _master?.Close();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Close();
        }

        public void Flush()
        {
            lock (_lock)
            {
                _master?.Flush();
            }
        }
    }

    public static class GuidExtensions {
        /// <summary>
        /// Render a GUID as a Base64 string
        /// </summary>
        [NotNull]public static string ShortString(this Guid g) {
            return Convert.ToBase64String(g.ToByteArray());
        }
    }


    internal class Header
    {
        public const long LOCKER_POS = 98;
        public const long HEADER_SIZE = 100;

        public const string FileID = "FileDB";        // 6 bytes
        public const short FileVersion = 1;           // 2 bytes

        /// <summary>
        /// Storage the first index page (root page). It's fixed on 0 (zero)
        /// Used to indicate start of binary file 
        /// <para></para>
        /// Armazena a primeira página que contem o inicio do indice. Valor sempre fixo = 0. Utilizado o inicio da busca binária
        /// </summary>
        public uint IndexRootPageID { get; set; }      // 4 bytes

        /// <summary>
        /// This last has free nodes to be used
        /// <para></para>
        /// Contem a página que possui espaço disponível para novas inclusões de indices
        /// </summary>
        public uint FreeIndexPageID { get; set; }      // 4 bytes
        
        /// <summary>
        /// When a deleted data, this variable point to first page emtpy. I will use to insert the next data page
        /// <para></para>
        /// Quando há exclusão de dados, a primeira pagina a ficar vazia infora a esse ponteiro que depois vai aproveitar numa proxima inclusão
        /// </summary>
        public uint FreeDataPageID { get; set; }       // 4 bytes

        /// <summary>
        /// Define, in a deleted data, the last deleted page. It's used to make continuos statments of empty page data
        /// <para></para>
        /// Define, numa exclusão de dados, a ultima pagina excluida. Será utilizado para fazer segmentos continuos de exclusão, ou seja, assim que um segundo arquivo for apagado, o ponteiro inicial dele deve apontar para o ponteiro final do outro
        /// </summary>
        public uint LastFreeDataPageID { get; set; }   // 4 bytes
        
        /// <summary>
        /// Last used page on FileDB disk (even index or data page). It's used to grow the file db (create new pages)
        /// <para></para>
        /// Ultima página utilizada pelo FileDB (seja para Indice/Data). É utilizado para quando o arquivo precisa crescer (criar nova pagina)
        /// </summary>
        public uint LastPageID { get; set; }           // 4 bytes

        public Header()
        {
            IndexRootPageID = uint.MaxValue;
            FreeIndexPageID = uint.MaxValue;
            FreeDataPageID = uint.MaxValue;
            LastFreeDataPageID = uint.MaxValue;
            LastPageID = uint.MaxValue;
            IsDirty = false;
        }

        public bool IsDirty { get; set; }
    }

    
    public sealed class FileDBStream : Stream
    {
        [NotNull]private readonly Engine _engine;

        private long _streamPosition;
        private DataPage _currentPage;
        private int _positionInPage;

        internal FileDBStream(Engine engine, Guid id)
        {
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));

            var indexNode = _engine.Search(id);
            if (indexNode != null)
            {
                Length = indexNode.FileLength;
                _currentPage = PageFactory.GetDataPage(indexNode.DataPageID, engine.Storage, false);
                FileInfo = new EntryInfo(indexNode);
            }
        }

        /// <summary>
        /// Get file information
        /// </summary>
        public EntryInfo FileInfo { get; }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override long Length { get; }

        public override long Position
        {
            get
            {
                return _streamPosition;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int bytesLeft = count;

            while (_currentPage != null && bytesLeft > 0)
            {
                int bytesToCopy = Math.Min(bytesLeft, _currentPage.DataBlockLength - _positionInPage);
                Buffer.BlockCopy(_currentPage.DataBlock, _positionInPage, buffer, offset, bytesToCopy);

                _positionInPage += bytesToCopy;
                bytesLeft -= bytesToCopy;
                offset += bytesToCopy;
                _streamPosition += bytesToCopy;

                if (_positionInPage >= _currentPage.DataBlockLength)
                {
                    _positionInPage = 0;

                    if (_currentPage.NextPageID == uint.MaxValue)
                        _currentPage = null;
                    else
                        _currentPage = PageFactory.GetDataPage(_currentPage.NextPageID, _engine.Storage, false);
                }
            }

            return count - bytesLeft;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }

    internal enum PageType { Invalid = 0, Data = 1, Index = 2 }
    internal class PageFactory
    {
        [NotNull]private static readonly object _pageLock = new object();
        public static void ReadIndexPageFromFile([NotNull]IndexPage indexPage, [NotNull]ThreadlockBinaryStream storage)
        {
            lock (_pageLock)
            {
                var reader = storage.AcquireReader();
                try
                {
                    // Seek the stream to the fist byte on page
                    long initPos = reader.BaseStream.Seek(Header.HEADER_SIZE + (indexPage.PageID * BasePage.PAGE_SIZE), SeekOrigin.Begin);

                    if (reader.ReadByte() != (byte)PageType.Index)
                        throw new Exception($"PageID {indexPage.PageID} is not a Index Page");

                    indexPage.NextPageID = reader.ReadUInt32();
                    indexPage.NodeIndex = reader.ReadByte();

                    // Seek the stream to end of header data page
                    reader.BaseStream.Seek(initPos + IndexPage.HEADER_SIZE, SeekOrigin.Begin);

                    for (int i = 0; i <= indexPage.NodeIndex; i++)
                    {
                        lock (indexPage)
                        {
                            var node = indexPage.Nodes[i];
                            if (node == null) continue;

                            node.ID = new Guid(reader.ReadBytes(16));

                            node.IsDeleted = reader.ReadBoolean();

                            if (node.Right != null)
                            {
                                node.Right.Index = reader.ReadByte();
                                node.Right.PageID = reader.ReadUInt32();
                            }
                            if (node.Left != null)
                            {
                                node.Left.Index = reader.ReadByte();
                                node.Left.PageID = reader.ReadUInt32();
                            }
                            node.DataPageID = reader.ReadUInt32();

                            node.FileName = Encoding.ASCII.GetString(reader.ReadBytes(IndexNode.FILENAME_SIZE));
                            node.FileLength = reader.ReadUInt32();
                        }
                    }
                }
                finally
                {
                    storage.Release(reader);
                }
            }
        }

        public static void WriteToFile([NotNull]IndexPage indexPage, [NotNull]ThreadlockBinaryStream storage)
        {
            lock (_pageLock)
            {
                lock (indexPage)
                {
                    var writer = storage.AcquireWriter();
                    try
                    {
                        // Seek the stream to the fist byte on page
                        long initPos = writer.BaseStream.Seek(Header.HEADER_SIZE + (indexPage.PageID * BasePage.PAGE_SIZE), SeekOrigin.Begin);

                        // Write page header 
                        writer.Write((byte)indexPage.Type);
                        writer.Write(indexPage.NextPageID);
                        writer.Write(indexPage.NodeIndex);

                        // Seek the stream to end of header index page
                        writer.BaseStream.Seek(initPos + IndexPage.HEADER_SIZE, SeekOrigin.Begin);

                        for (int i = 0; i <= indexPage.NodeIndex; i++)
                        {
                            lock (indexPage)
                            {
                                var node = indexPage.Nodes[i];
                                if (node == null) continue;

                                writer.Write(node.ID.ToByteArray());

                                writer.Write(node.IsDeleted);

                                if (node.Right != null)
                                {
                                    writer.Write(node.Right.Index);
                                    writer.Write(node.Right.PageID);
                                }
                                if (node.Left != null)
                                {
                                    writer.Write(node.Left.Index);
                                    writer.Write(node.Left.PageID);
                                }
                                writer.Write(node.DataPageID);

                                writer.Write(node.FileName.LimitedByteString(IndexNode.FILENAME_SIZE));
                                writer.Write(node.FileLength);
                            }
                        }
                    }
                    finally
                    {
                        storage.Release(writer);
                    }
                }
            }
        }

        private static void EnsurePageIsDataType(byte pageType, uint pageID) {

            switch (pageType) {
                case (byte)PageType.Data: break;

                case (byte)PageType.Index:
                    throw new Exception($"PageID {pageID} should be a Data Page, but is an index. The index is possibly corrupt.");

                case (byte)PageType.Invalid:
                    throw new Exception($"PageID {pageID} should be a Data Page, but is not valid. This might be a data race in the database");

                default:
                    throw new Exception($"PageID {pageID} is not a Data Page -- expected {(byte)PageType.Data}, but got {pageType}");
            }
            }

        public static bool ReadDataPageFromFile([NotNull]DataPage dataPage, [NotNull]ThreadlockBinaryStream storage, bool onlyHeader)
        {
            lock (_pageLock)
            {
                lock (dataPage)
                {
                    var reader = storage.AcquireReader();
                    try
                    {
                        // Seek the stream on first byte from data page
                        var target = Header.HEADER_SIZE + (dataPage.PageID * BasePage.PAGE_SIZE);
                        
                        var initPos = reader.BaseStream.Seek(Header.HEADER_SIZE + (dataPage.PageID * BasePage.PAGE_SIZE), SeekOrigin.Begin);

                        if (target != initPos) throw new Exception("Unexpected end of file");

                        // This happens when writes happen in parallel
                        if (reader.BaseStream.Position >= reader.BaseStream.Length)
                            throw new Exception($"File position run-out. Expected {reader.BaseStream.Position}, but limit is {reader.BaseStream.Length}.");

                        var pageType = reader.ReadByte();
                        EnsurePageIsDataType(pageType, dataPage.PageID);

                        dataPage.NextPageID = reader.ReadUInt32();
                        dataPage.IsEmpty = reader.ReadBoolean();
                        dataPage.DataBlockLength = reader.ReadInt16();

                        // If page is empty or onlyHeader parameter, I don't read data content
                        if (!dataPage.IsEmpty && !onlyHeader)
                        {
                            // Seek the stream at the end of page header
                            reader.BaseStream.Seek(initPos + DataPage.HEADER_SIZE, SeekOrigin.Begin);

                            // Read all bytes from page
                            dataPage.SetBlock(reader.ReadBytes(dataPage.DataBlockLength));
                        }
                        return true;
                    }
                    finally
                    {
                        storage.Release(reader);
                    }
                }
            }
        }

        public static void WriteToFile([NotNull]DataPage dataPage, [NotNull]ThreadlockBinaryStream storage)
        {
            lock (_pageLock)
            {
                var writer = storage.AcquireWriter();
                try
                {
                    // Seek the stream on first byte from data page
                    long initPos = writer.BaseStream.Seek(Header.HEADER_SIZE + (dataPage.PageID * BasePage.PAGE_SIZE), SeekOrigin.Begin);

                    // Write data page header
                    writer.Write((byte)dataPage.Type);
                    writer.Write(dataPage.NextPageID);
                    writer.Write(dataPage.IsEmpty);
                    writer.Write(dataPage.DataBlockLength);

                    // Seek the stream at the end of page header
                    writer.BaseStream.Seek(initPos + DataPage.HEADER_SIZE, SeekOrigin.Begin);

                    writer.Write(dataPage.DataBlock, 0, dataPage.DataBlockLength);
                }
                finally
                {
                    storage.Release(writer);
                }
            }
        }

        [NotNull]
        public static IndexPage GetIndexPage(uint pageID, [NotNull]ThreadlockBinaryStream reader)
        {
            lock (_pageLock)
            {
                var indexPage = new IndexPage(pageID);
                ReadIndexPageFromFile(indexPage, reader);
                return indexPage;
            }
        }

        [NotNull]public static DataPage GetDataPage(uint pageID, [NotNull]ThreadlockBinaryStream reader, bool onlyHeader)
        {
            lock (_pageLock)
            {
                var dataPage = new DataPage(pageID);
                dataPage.Valid = ReadDataPageFromFile(dataPage, reader, onlyHeader);
                return dataPage;
            }
        }

        public static BasePage GetBasePage(uint pageID, [NotNull]ThreadlockBinaryStream storage)
        {
            lock (_pageLock)
            {
                var pageType = ReadPageType(pageID, storage);
                return pageType == (byte)PageType.Index
                    ? (BasePage)GetIndexPage(pageID, storage)
                    : GetDataPage(pageID, storage, true);
            }
        }

        private static byte ReadPageType(uint pageID, [NotNull]ThreadlockBinaryStream storage)
        {
            byte pageType;
            var reader = storage.AcquireReader();
            try
            {
                reader.BaseStream.Seek(Header.HEADER_SIZE + (pageID * BasePage.PAGE_SIZE), SeekOrigin.Begin);
                pageType = reader.ReadByte();
            }
            finally
            {
                storage.Release(reader);
            }

            return pageType;
        }
    }


    internal delegate void ReleasePageIndexFromCache(IndexPage page);

    internal class CacheIndexPage
    {
        public const int CACHE_SIZE = 200;
        [NotNull]private static readonly object _cacheLock = new object();

        [NotNull]private readonly ThreadlockBinaryStream _storage;
        [NotNull]private readonly Dictionary<uint, IndexPage> _cache;
        private readonly uint _rootPageID;

        public CacheIndexPage(ThreadlockBinaryStream storage, uint rootPageID)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage)); 
            _cache = new Dictionary<uint,IndexPage>();
            _rootPageID = rootPageID;
        }

        [NotNull]public IndexPage GetPage(uint pageID)
        {
            lock (_cacheLock)
            {
                if (_cache.ContainsKey(pageID)) return _cache[pageID] ?? throw new InvalidOperationException("Thread race in page cache");

                var indexPage = PageFactory.GetIndexPage(pageID, _storage);

                AddPage(indexPage, false);

                return indexPage;
            }
        }

        public void AddPage([NotNull]IndexPage indexPage, bool markAsDirty)
        {
            lock (_cacheLock)
            {
                if (!_cache.ContainsKey(indexPage.PageID))
                {
                    if (_cache.Count >= CACHE_SIZE)
                    {
                        // Remove first page that is not the root page (because I use too much)
                        var pageToRemove = _cache.FirstOrDefault(x => x.Key != _rootPageID);
                        if (pageToRemove.Value == null) throw new Exception("Could not find a non-root page");

                        if (pageToRemove.Value.IsDirty)
                        {
                            PageFactory.WriteToFile(pageToRemove.Value, _storage);
                            pageToRemove.Value.IsDirty = false;
                        }

                        _cache.Remove(pageToRemove.Key);
                    }

                    _cache.Add(indexPage.PageID, indexPage);
                }

                if (markAsDirty) indexPage.IsDirty = true;
            }
        }

        public void PersistPages()
        {
            lock (_cacheLock)
            {
                // Check which pages is dirty and need to saved on disk 

                var pagesToPersist = _cache.Values.Where(x => x?.IsDirty == true).ToArray();
                if (pagesToPersist.Length <= 0) return;

                foreach (var indexPage in pagesToPersist)
                {
                    PageFactory.WriteToFile(indexPage, _storage);
                    indexPage.IsDirty = false;
                }
            }
        }
    }


    internal abstract class BasePage
    {
        public const long PAGE_SIZE = 4096;

        public uint PageID { get; set; }
        public abstract PageType Type { get; }
        public uint NextPageID { get; set; }
    }

    internal class IndexPage : BasePage
    {
        public const long HEADER_SIZE = 46;
        public const int NODES_PER_PAGE = 50;

        public override PageType Type { get { return PageType.Index; } }  //  1 byte
        public byte NodeIndex { get; set; }                               //  1 byte

        [NotNull]public IndexNode[] Nodes { get; set; }

        public bool IsDirty { get; set; }

        public IndexPage(uint pageID)
        {
            PageID = pageID;
            NextPageID = uint.MaxValue;
            NodeIndex = 0;
            Nodes = new IndexNode[NODES_PER_PAGE];
            IsDirty = false;

            for (int i = 0; i < NODES_PER_PAGE; i++)
            {
                Nodes[i] = new IndexNode(this);
            }
        }

    }

    internal class IndexLink
    {
        public byte Index { get; set; }
        public uint PageID { get; set; }

        public IndexLink()
        {
            Index = 0;
            PageID = uint.MaxValue;
        }

        public bool IsEmpty
        {
            get
            {
                return PageID == uint.MaxValue;
            }
        }
    }

    internal class DataPage : BasePage
    {
        public const long HEADER_SIZE = 8;
        public const long DATA_PER_PAGE = 4088;

        public override PageType Type { get { return PageType.Data; } }  //  1 byte

        public bool IsEmpty { get; set; }                                //  1 byte
        public short DataBlockLength { get; set; }                       //  2 bytes

        [NotNull]public byte[] DataBlock { get; private set;}

        /// <summary>
        /// Indicates the page has had data loaded in correctly
        /// </summary>
        public bool Valid { get; set; }

        public DataPage(uint pageID)
        {
            PageID = pageID;
            IsEmpty = true;
            DataBlockLength = 0;
            NextPageID = uint.MaxValue;
            DataBlock = new byte[DATA_PER_PAGE];
            Valid = true;
        }

        public void SetBlock(byte[] readBytes)
        {
            if (readBytes != null) DataBlock = readBytes;
        }
    }

    internal class IndexNode
    {
        // NOTE: this seems to be wasting a *lot* of space by forcing the file name to something small
        public const int FILENAME_SIZE = 255;       // Size of file name string
        public const int INDEX_NODE_SIZE = 81;     // Node Index size

        public Guid ID { get; set; }               // 16 bytes

        public bool IsDeleted { get; set; }        //  1 byte

        public IndexLink Right { get; set; }       //  5 bytes 
        public IndexLink Left { get; set; }        //  5 bytes

        public uint DataPageID { get; set; }       //  4 bytes

        // Info
        public string FileName { get; set; }       // 41 bytes (file name + extension) -- this could be 46 bytes or more
        public uint FileLength { get; set; }       //  4 bytes

        public IndexPage IndexPage { get; set; }

        public IndexNode(IndexPage indexPage)
        {
            ID = Guid.Empty;
            IsDeleted = true; // Start with index node mark as deleted. Update this after save all stream on disk
            Right = new IndexLink();
            Left = new IndexLink();
            DataPageID = uint.MaxValue;
            IndexPage = indexPage;
        }

        public void UpdateFromEntry([NotNull]EntryInfo entity)
        {
            ID = entity.ID;
            FileName = entity.FileName;
            FileLength = entity.FileLength;
        }
    }

    
    internal static class BinaryWriterExtensions
    {
        private const int MAX_TRY_LOCK_FILE = 50; // Max try to lock the data file
        private const int DELAY_TRY_LOCK_FILE = 50; // in miliseconds

        
        [NotNull]public static byte[] LimitedByteString(this string str, int size)
        {
            var buffer = new byte[size];
            var strbytes = Encoding.ASCII.GetBytes(str??"");

            Array.Copy(strbytes, buffer, size > strbytes.Length ? strbytes.Length : size);

            return buffer;
        }


        public static void Write(this BinaryWriter writer, Guid guid)
        {
            writer?.Write(guid.ToByteArray());
        }

        public static void Write(this BinaryWriter writer, DateTime dateTime)
        {
            writer?.Write(dateTime.Ticks);
        }

        public static long Seek(this BinaryWriter writer, long position)
        {
            if (writer == null) throw new Exception("Tried to seek a null writer");
            return writer.BaseStream.Seek(position, SeekOrigin.Begin);
        }

        public static void Lock(this BinaryWriter writer, long position, long length)
        {
            if (writer == null) throw new Exception("Tried to lock a null writer");
            var fileStream = writer.BaseStream as FileStream;

            if (fileStream != null) TryLockFile(fileStream, position, length, 0);
        }

        public static bool IsLockException(IOException exception)
        {
            int errorCode = Marshal.GetHRForException(exception) & ((1 << 16) - 1);
            return errorCode == 32 || errorCode == 33;
        }


        public static void TryLockFile([NotNull]this FileStream fileStream, long position, long length, int tryCount)
        {
            try
            {
                fileStream.Lock(position, length);
            }
            catch (IOException ex)
            {
                if (IsLockException(ex))
                {
                    if (tryCount >= MAX_TRY_LOCK_FILE)
                        throw new Exception("Database file is in lock for a long time");

                    Thread.Sleep(tryCount * DELAY_TRY_LOCK_FILE);

                    TryLockFile(fileStream, position, length, ++tryCount);
                }
                else throw;
            }
        }

        public static void Unlock(this BinaryWriter writer, long position, long length)
        {
            var fileStream = writer?.BaseStream as FileStream;
            fileStream?.Unlock(position, length);
        }
    }
    
    internal class DataFactory
    {
        [NotNull] private static readonly object _pageLock = new object();

        public static uint GetStartDataPageID([NotNull]Engine engine)
        {
            lock (_pageLock)
            {
                lock (engine.Header)
                {
                    if (engine.Header.FreeDataPageID != uint.MaxValue) // I have free page inside the disk file. Use it
                    {
                        // Take the first free data page
                        var startPage = PageFactory.GetDataPage(engine.Header.FreeDataPageID, engine.Storage, true);

                        engine.Header.FreeDataPageID = startPage.NextPageID; // and point the free page to new free one

                        // If the next page is MAX, fix LastFreeData too

                        if (engine.Header.FreeDataPageID == uint.MaxValue)
                            engine.Header.LastFreeDataPageID = uint.MaxValue;

                        return startPage.PageID;
                    }
                    // Don't have free data pages, create new one.
                    engine.Header.LastPageID++;
                    return engine.Header.LastPageID;
                }
            }
        }

        // Take a new data page on sequence and update the last
        [NotNull]public static DataPage GetNewDataPage([NotNull]DataPage basePage, [NotNull]Engine engine)
        {
            lock (_pageLock)
            {
                lock (engine.Header)
                {
                    if (basePage.NextPageID != uint.MaxValue)
                    {
                        PageFactory.WriteToFile(basePage, engine.Storage); // Write last page on disk

                        var dataPage = PageFactory.GetDataPage(basePage.NextPageID, engine.Storage, false);

                        engine.Header.FreeDataPageID = dataPage.NextPageID;

                        if (engine.Header.FreeDataPageID == uint.MaxValue)
                            engine.Header.LastFreeDataPageID = uint.MaxValue;

                        return dataPage;
                    }

                    var pageID = ++engine.Header.LastPageID;
                    var newPage = new DataPage(pageID);
                    basePage.NextPageID = newPage.PageID;
                    PageFactory.WriteToFile(basePage, engine.Storage); // Write last page on disk
                    return newPage;
                }
            }
        }

        public static void InsertFile([NotNull]IndexNode node, [NotNull]Stream stream, [NotNull]Engine engine)
        {
            lock (_pageLock)
            {
                lock (node)
                {
                    lock (engine.Header)
                    {
                        var buffer = new byte[DataPage.DATA_PER_PAGE];
                        uint totalBytes = 0;

                        int read;
                        int dataPerPage = (int)DataPage.DATA_PER_PAGE;
                        var dataPage = engine.GetPageData(node.DataPageID);

                        while ((read = stream.Read(buffer, 0, dataPerPage)) > 0)
                        {
                            if (totalBytes > 0) { dataPage = GetNewDataPage(dataPage, engine); }

                            totalBytes += (uint)read;

                            if (!dataPage.IsEmpty) throw new Exception($"Page {dataPage.PageID} is not empty");

                            Array.Copy(buffer, dataPage.DataBlock, read);
                            dataPage.IsEmpty = false;
                            dataPage.DataBlockLength = (short)read;
                            PageFactory.WriteToFile(dataPage, engine.Storage);
                        }

                        // If the last page point to another one, i need to fix that
                        if (dataPage.NextPageID != uint.MaxValue)
                        {
                            engine.Header.FreeDataPageID = dataPage.NextPageID;
                            dataPage.NextPageID = uint.MaxValue;
                        }

                        // Save the last page on disk
                        PageFactory.WriteToFile(dataPage, engine.Storage);

                        // Save on node index that file length
                        node.FileLength = totalBytes;
                    }
                }
            }
        }

        public static void ReadFile([NotNull]IndexNode node, [NotNull]Stream stream, [NotNull]Engine engine)
        {
            var dataPage = PageFactory.GetDataPage(node.DataPageID, engine.Storage, false);

            while (dataPage != null)
            {
                stream.Write(dataPage.DataBlock, 0, dataPage.DataBlockLength);

                if (dataPage.NextPageID == uint.MaxValue)
                    dataPage = null;
                else
                    dataPage = PageFactory.GetDataPage(dataPage.NextPageID, engine.Storage, false);
            }

        }

        public static void MarkAsEmpty(uint firstPageID, [NotNull]Engine engine)
        {
            lock (_pageLock)
            {
                var dataPage = PageFactory.GetDataPage(firstPageID, engine.Storage, true);
                uint lastPageID = uint.MaxValue;
                var cont = true;

                while (cont)
                {
                    dataPage.IsEmpty = true;

                    PageFactory.WriteToFile(dataPage, engine.Storage);

                    if (dataPage.NextPageID != uint.MaxValue)
                    {
                        lastPageID = dataPage.NextPageID;
                        dataPage = PageFactory.GetDataPage(lastPageID, engine.Storage, true);
                    }
                    else
                    {
                        cont = false;
                    }
                }

                lock (engine.Header)
                {
                    // Fix header to correct pointer
                    if (engine.Header.FreeDataPageID == uint.MaxValue) // No free pages
                    {
                        engine.Header.FreeDataPageID = firstPageID;
                        engine.Header.LastFreeDataPageID = lastPageID == uint.MaxValue ? firstPageID : lastPageID;
                    }
                    else
                    {
                        // Take the last statment available
                        var lastPage = PageFactory.GetDataPage(engine.Header.LastFreeDataPageID, engine.Storage, true);

                        // Point this last statent to first of next one
                        if (lastPage.NextPageID != uint.MaxValue || !lastPage.IsEmpty) // This is never to happend!!
                            throw new Exception("The page is not empty");

                        // Update this last page to first new empty page
                        lastPage.NextPageID = firstPageID;

                        // Save on disk this update
                        PageFactory.WriteToFile(lastPage, engine.Storage);

                        // Point header to the new empty page
                        engine.Header.LastFreeDataPageID = lastPageID == uint.MaxValue ? firstPageID : lastPageID;
                    }
                }
            }
        }

    }

    internal class HeaderFactory
    {
        [NotNull]private static readonly object _lock = new object();

        public static void ReadFromFile([NotNull]Header header, [NotNull]ThreadlockBinaryStream storage)
        {
            lock (_lock)
            {
                lock (header)
                {
                    var reader = storage.AcquireReader();
                    try
                    {
                        // Seek the stream on 0 position to read header
                        reader.BaseStream.Seek(0, SeekOrigin.Begin);

                        // Make same validation on header file
                        if (Encoding.ASCII.GetString(reader.ReadBytes(Header.FileID.Length)) != Header.FileID)
                            throw new Exception("The file is not a valid storage archive");

                        if (reader.ReadInt16() != Header.FileVersion)
                            throw new Exception("The archive version is not valid");

                        header.IndexRootPageID = reader.ReadUInt32();
                        header.FreeIndexPageID = reader.ReadUInt32();
                        header.FreeDataPageID = reader.ReadUInt32();
                        header.LastFreeDataPageID = reader.ReadUInt32();
                        header.LastPageID = reader.ReadUInt32();
                        header.IsDirty = false;
                    }
                    finally
                    {
                        storage.Release(reader);
                    }
                }
            }
        }

        public static void WriteToFile([NotNull]Header header, [NotNull]ThreadlockBinaryStream storage)
        {
            lock (_lock)
            {
                lock (header)
                {
                    var writer = storage.AcquireWriter();
                    try
                    {
                        // Seek the stream on 0 position to save header
                        writer.BaseStream.Seek(0, SeekOrigin.Begin);

                        writer.Write(Header.FileID.LimitedByteString(Header.FileID.Length));
                        writer.Write(Header.FileVersion);

                        writer.Write(header.IndexRootPageID);
                        writer.Write(header.FreeIndexPageID);
                        writer.Write(header.FreeDataPageID);
                        writer.Write(header.LastFreeDataPageID);
                        writer.Write(header.LastPageID);
                    }
                    finally
                    {
                        storage.Release(writer);
                    }
                }
            }
        }

    }
    internal class IndexFactory
    {
        [NotNull]private static readonly object _indexLock = new object();

        public static IndexNode GetRootIndexNode([NotNull]Engine engine)
        {
            lock (_indexLock)
            {
                lock (engine.Header)
                {
                    var rootIndexPage = engine.CacheIndexPage.GetPage(engine.Header.IndexRootPageID);
                    return rootIndexPage.Nodes[0];
                }
            }
        }

        public static IndexNode BinaryInsert([NotNull]EntryInfo target, [NotNull]IndexNode baseNode, [NotNull]Engine engine)
        {
            lock (_indexLock)
            {

                var dif = baseNode.ID.CompareTo(target.ID);
                if (baseNode.Right == null || baseNode.Left == null) throw new Exception("Index node structure is broken");

                switch (dif)
                {
                    // > Greater (Right)
                    case 1:
                        return baseNode.Right.IsEmpty
                            ? BinaryInsertNode(baseNode.Right, baseNode, target, engine)
                            : BinaryInsert(target, GetChildIndexNode(baseNode.Right, engine), engine);
                    // < Less (Left)
                    case -1:
                        return baseNode.Left.IsEmpty
                            ? BinaryInsertNode(baseNode.Left, baseNode, target, engine)
                            : BinaryInsert(target, GetChildIndexNode(baseNode.Left, engine), engine);
                    default:
                        throw new Exception("GUID collision.");
                }
            }
        }

        [NotNull]private static IndexNode GetChildIndexNode([NotNull]IndexLink link, [NotNull]Engine engine)
        {
            lock (_indexLock)
            {
                var pageIndex = engine.CacheIndexPage.GetPage(link.PageID);
                return pageIndex.Nodes[link.Index] ?? throw new Exception("Index node structure is incomplete");
            }
        }

        private static IndexNode BinaryInsertNode([NotNull]IndexLink baseLink, [NotNull]IndexNode baseNode, [NotNull]EntryInfo entry, [NotNull]Engine engine)
        {
            lock (_indexLock)
            {
                // Insert new node
                var pageIndex = engine.GetFreeIndexPage();
                var newNode = pageIndex?.Nodes[pageIndex.NodeIndex];
                if (newNode == null) throw new Exception("Failed to find new node during BinaryInsertNode");
                if (baseNode.IndexPage == null) throw new Exception("Invalid page structure in BinaryInsertNode");


                lock (newNode)
                {
                    baseLink.PageID = pageIndex.PageID;
                    baseLink.Index = pageIndex.NodeIndex;

                    newNode.UpdateFromEntry(entry);
                    newNode.DataPageID = DataFactory.GetStartDataPageID(engine);

                    if (pageIndex.PageID != baseNode.IndexPage.PageID) engine.CacheIndexPage.AddPage(baseNode.IndexPage, true);

                    engine.CacheIndexPage.AddPage(pageIndex, true);

                    return newNode;
                }
            }
        }

        [CanBeNull]
        public static IndexNode BinarySearch(Guid target, [NotNull]IndexNode baseNode, [NotNull]Engine engine)
        {
            lock (_indexLock)
            {
                var dif = baseNode.ID.CompareTo(target);
                if (baseNode.Right == null || baseNode.Left == null) throw new Exception("Incomplete structure in BinarySearch");

                switch (dif)
                {
                    // > Maior (Right)
                    case 1:
                        return baseNode.Right.IsEmpty
                            ? null
                            : BinarySearch(target, GetChildIndexNode(baseNode.Right, engine), engine);
                    // < Menor (Left)
                    case -1:
                        return baseNode.Left.IsEmpty
                            ? null
                            : BinarySearch(target, GetChildIndexNode(baseNode.Left, engine), engine);
                    default:
                        // Found it
                        return baseNode;
                }
            }
        }


    }


    public class EntryInfo
    {
        public Guid ID { get; }
        public string FileName { get; }
        public uint FileLength { get; internal set; }

        internal EntryInfo(string fileName)
        {
            ID = Guid.NewGuid();
            FileName = Clean(fileName);
            FileLength = 0;
        }

        // Remove trailing \u0000 characters.
        private string Clean(string fileName)
        {
            return fileName?.TrimEnd('\u0000');
        }

        internal EntryInfo([NotNull]IndexNode node)
        {
            ID = node.ID;
            FileName = Clean(node.FileName);
            FileLength = node.FileLength;
        }
    }

        internal class Engine : IDisposable
    {
        [NotNull] public ThreadlockBinaryStream Storage { get; }
        [NotNull]public CacheIndexPage CacheIndexPage { get; } // Used for cache index pages.
        [NotNull]public Header Header { get; }

        [NotNull] private readonly object _globalLock = new object(); // aggressive lock -- use as infrequently as possible

        public Engine([NotNull]Stream stream)
        {
            if (!stream.CanWrite) throw new Exception("A read/write stream is required");
            Storage = new ThreadlockBinaryStream(stream);

            Header = new Header();
            HeaderFactory.ReadFromFile(Header, Storage);

            CacheIndexPage = new CacheIndexPage(Storage, Header.IndexRootPageID);
        }

        public IndexPage GetFreeIndexPage()
        {
            lock (Header)
            {
                var freeIndexPage = CacheIndexPage.GetPage(Header.FreeIndexPageID);

                lock (freeIndexPage)
                {
                    // Check if "free page" has no more index to be used
                    if (freeIndexPage.NodeIndex >= IndexPage.NODES_PER_PAGE - 1)
                    {
                        Header.LastPageID++; // Take last page and increase
                        Header.IsDirty = true;

                        var newIndexPage = new IndexPage(Header.LastPageID); // Create a new index page
                        freeIndexPage.NextPageID = newIndexPage.PageID; // Point older page to the new page
                        Header.FreeIndexPageID = Header.LastPageID; // Update last index page

                        CacheIndexPage.AddPage(freeIndexPage, true);

                        return newIndexPage;
                    }

                    // Has more free index on same index page? return them
                    freeIndexPage.NodeIndex++; // Reserve space
                    return freeIndexPage;
                }
            }
        }

        [NotNull]public DataPage GetPageData(uint pageID)
        {
            if (pageID == Header.LastPageID) // Page does not exists in disk
            {
                var dataPage = new DataPage(pageID);
                return dataPage;
            }

            return PageFactory.GetDataPage(pageID, Storage, false);
        }

        public void Write([NotNull]EntryInfo entry, [NotNull]Stream stream)
        {
            // Take the first index page
            IndexNode indexNode;
            lock (_globalLock)
            {

                var rootIndexNode = IndexFactory.GetRootIndexNode(this);
                if (rootIndexNode == null) throw new Exception("Could not find root index node: database is corrupt");

                // Search and insert the index
                indexNode = IndexFactory.BinaryInsert(entry, rootIndexNode, this);
                if (indexNode == null) throw new Exception("Could not insert new index node");
            }

            // In this moment, the index are ready and saved. I use to add the file
            lock (_globalLock)
            {
                DataFactory.InsertFile(indexNode, stream, this);
                Storage.Flush();
            }

            // Update entry information with file length (I know file length only after read all)
            entry.FileLength = indexNode.FileLength;

            // Only after insert all stream file I confirm that index node is valid
            indexNode.IsDeleted = false;

            // Mask header as dirty for save on dispose
            lock (Header)
            {
                Header.IsDirty = true;
            }
        }

        public IndexNode Search(Guid id)
        {
            // Take the root node from inital index page
            var rootIndexNode = IndexFactory.GetRootIndexNode(this);
            if (rootIndexNode == null) throw new Exception("Could not find root index node: database is corrupt");

            var indexNode = IndexFactory.BinarySearch(id, rootIndexNode, this);

            // Returns null with not found the record, return false
            if (indexNode == null || indexNode.IsDeleted)
                return null;

            return indexNode;
        }

        public EntryInfo Read(Guid id, [NotNull]Stream stream)
        {
            // Search from index node
            var indexNode = Search(id);

            // If index node is null, not found the guid
            if (indexNode == null) return null;

            // Create a entry based on index node
            var entry = new EntryInfo(indexNode);

            // Read data from the index pointer to stream
            DataFactory.ReadFile(indexNode, stream, this);

            return entry;
        }

        public FileDBStream OpenRead(Guid id)
        {
            // Open a FileDBStream and return to user
            var file = new FileDBStream(this, id);

            // If FileInfo is null, ID was not found
            return file.FileInfo == null ? null : file;
        }

        public bool Delete(Guid id)
        {
            // Search index node from guid
            var indexNode = Search(id);

            // If null, not found (return false)
            if (indexNode?.IndexPage == null) return false;

            // Delete the index node logicaly
            indexNode.IsDeleted = true;

            // Add page (from index node) to cache and set as dirty
            CacheIndexPage.AddPage(indexNode.IndexPage, true);

            // Mark all data blocks (from data pages) as IsEmpty = true
            DataFactory.MarkAsEmpty(indexNode.DataPageID, this);

            // Set header as Dirty to be saved on dispose
            Header.IsDirty = true;

            return true; // Confirm deletion
        }

        public EntryInfo[] ListAllFiles()
        {
            // Get root index page from cache
            var pageIndex = CacheIndexPage.GetPage(Header.IndexRootPageID);

            var list = new List<EntryInfo>();

            while (true)
            {
                for (int i = 0; i <= pageIndex.NodeIndex; i++)
                {
                    // Convert node (if is not logicaly deleted) to Entry
                    var node = pageIndex.Nodes[i];
                    if (node != null && !node.IsDeleted) list.Add(new EntryInfo(node));
                }

                if (pageIndex.NextPageID == uint.MaxValue) break;

                // Go to the next page
                pageIndex = CacheIndexPage.GetPage(pageIndex.NextPageID);
            }

            return list.ToArray();
        }

        public void PersistPages()
        {
            // Check if header is dirty and save to disk
            if (Header.IsDirty)
            {
                lock (Header)
                {
                    lock (_globalLock)
                    {
                        HeaderFactory.WriteToFile(Header, Storage);
                        Header.IsDirty = false;
                    }
                }
            }

            // Persist all index pages that are dirty
            CacheIndexPage.PersistPages();
        }

        public void Dispose()
        {
            Storage.Close();
        }

        public static void CreateEmptyFile([NotNull]Stream storageStream)
        {
            // Create new header instance
            var header = new Header
            {
                IndexRootPageID = 0,
                FreeIndexPageID = 0,
                FreeDataPageID = uint.MaxValue,
                LastFreeDataPageID = uint.MaxValue,
                LastPageID = 0
            };


            using (var storage = new ThreadlockBinaryStream(storageStream, closeBaseStream: false))
            {
                HeaderFactory.WriteToFile(header, storage);

                // Create a first fixed index page
                var pageIndex = new IndexPage(0) { NodeIndex = 0, NextPageID = uint.MaxValue };

                // Create first fixed index node, with fixed middle guid
                var indexNode = pageIndex.Nodes[0];
                if (indexNode == null) throw new Exception("Failed to create primary index node");

                lock (indexNode)
                {
                    indexNode.ID = new Guid(new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127 });
                    indexNode.IsDeleted = true;
                    indexNode.Right = new IndexLink();
                    indexNode.Left = new IndexLink();
                    indexNode.DataPageID = uint.MaxValue;
                    indexNode.FileName = string.Empty;
                }
                PageFactory.WriteToFile(pageIndex, storage);
            }
            storageStream.Flush();
        }

    }


    
}
