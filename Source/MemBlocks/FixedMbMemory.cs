using System.Collections;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Text.Json;

namespace MemBlocks;

internal class FixedMbMemory<T> : IDisposable, IAsyncDisposable, IEnumerable<T> where T : class
{
    public string Name { get; }
    public int Capacity { get; }
    public int BlockSize { get; }
    public int Size { get; }

    private readonly FileStream _mmfStream;
    private readonly MemoryMappedFile _mmf;

    public FixedMbMemory(string name, int capacity, int blockSize)
    {
        if (blockSize > capacity)
        {
            throw new ArgumentException("Block size cannot exceed capacity.");
        }

        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Name cannot be empty or null.");
        }

        Name = name;
        Capacity = capacity;
        BlockSize = blockSize;
        Size = (int) Math.Floor((double) capacity / blockSize);

        var mmfPath = Path.Join(Path.GetTempPath(), $"{Name}-fmbm-mmf.dat");
        
        _mmfStream = new FileStream(mmfPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, BlockSize);
        _mmf = MemoryMappedFile.CreateFromFile(_mmfStream, null, Capacity, MemoryMappedFileAccess.ReadWrite, HandleInheritability.Inheritable, true);
    }

    public bool TryRead(int index, out T? item, int timeout = 150)
    {
        ThrowIfIndexOutOfRange(index);
        
        item = default;

        using var mutex = GetItemMutex(index);
        var lockAchieved = mutex.WaitOne(timeout);

        if (!lockAchieved)
        {
            return false;
        }

        try
        {
            using var stream = GetItemStream(index);
            item = ReadItemAsync(stream).GetAwaiter().GetResult();
        }
        finally
        {
            mutex.ReleaseMutex();
        }

        return true;
    }

    public async Task<T?> ReadAsync(int index)
    {
        ThrowIfIndexOutOfRange(index);

        using var mutex = GetItemMutex(index);

        mutex.WaitOne();

        try
        {
            await using var stream = GetItemStream(index);
            return await ReadItemAsync(stream);
        }
        finally
        {
            mutex.ReleaseMutex();
        }
    }

    public async Task WriteAsync(int index, T item)
    {
        ThrowIfIndexOutOfRange(index);

        if (item == null)
        {
            throw new ArgumentException("Item cannot be null. To delete an item call Delete instead.");
        }

        using var mutex = GetItemMutex(index);

        mutex.WaitOne();

        try
        {
            await using var stream = GetItemStream(index);
            await WriteItemAsync(stream, item);
        }
        finally
        {
            mutex.ReleaseMutex();
        }
    }

    public async Task DeleteAsync(int index)
    {
        ThrowIfIndexOutOfRange(index);

        using var mutex = GetItemMutex(index);

        mutex.WaitOne();
        
        try
        {
            await using var stream = GetItemStream(index);
            await DeleteItemAsync(stream);
        }
        finally
        {
            mutex.ReleaseMutex();
        }
    }

    public async Task ClearAsync()
    {
        var indexes = Enumerable.Range(0, Size);
        
        await Parallel.ForEachAsync(indexes, async (index, _) =>
        {
            await using var stream = GetItemStream(index);
            await DeleteItemAsync(stream);
        });
    }

    public void Dispose()
    {
        _mmf.Dispose();
        _mmfStream.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        _mmf.Dispose();
        await _mmfStream.DisposeAsync();
    }

    public IEnumerator<T> GetEnumerator()
    {
        return new FixedMbMemoryEnumerator<T>(this);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    private void ThrowIfIndexOutOfRange(int index)
    {
        if (index > Size || index < 0)
        {
            throw new IndexOutOfRangeException();
        }
    }

    private Mutex GetItemMutex(int index)
        => new(false, $"{Name}-{index}-fmbm-mmf");

    private Stream GetItemStream(int index)
        => _mmf.CreateViewStream(BlockSize * index, BlockSize, MemoryMappedFileAccess.ReadWrite);

    private async Task WriteItemAsync(Stream stream, T item)
    {
        try
        {
            stream.Seek(0, SeekOrigin.Begin);

            var dataBytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(item));
            
            if (dataBytes.Length > BlockSize)
            {
                throw new ArgumentOutOfRangeException($"Item provided exceeds block capacity. Increase block size or reduce item size. Serialized item size was \"{dataBytes.Length}\". Block size was \"{BlockSize}\".");
            }
            
            var paddingBytes = new byte[BlockSize - dataBytes.Length];

            Array.Fill(paddingBytes, (byte) 0x00);

            await stream.WriteAsync(dataBytes);
            await stream.WriteAsync(paddingBytes);
        }
        catch
        {
            await DeleteItemAsync(stream);
            throw;
        }
    }
    
    private async Task<T?> ReadItemAsync(Stream stream)
    {
        try
        {
            stream.Seek(0, SeekOrigin.Begin);

            var dataBytes = new byte[BlockSize];

            _ = await stream.ReadAsync(dataBytes);
            dataBytes = dataBytes.Where(x => x != 0x00).ToArray();

            return JsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(dataBytes));
        }
        catch
        {
            await DeleteItemAsync(stream);
            throw;
        }
    }

    private async Task DeleteItemAsync(Stream stream)
    {
        stream.Seek(0, SeekOrigin.Begin);
        
        var dataBytes = new byte[BlockSize];
        
        Array.Fill(dataBytes, (byte)0x00);

        await stream.WriteAsync(dataBytes);
    }

}