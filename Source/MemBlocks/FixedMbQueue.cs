namespace MemBlocks;

public class FixedMbQueue<T> : IDisposable, IAsyncDisposable where T : class
{
    private readonly FixedMbMemory<FixedMbQueueMeta> _metaMemory;
    private readonly FixedMbMemory<T> _itemMemory;
    private readonly Mutex _mutex;

    public FixedMbQueue(string name, int itemSize, int queueSize)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Name cannot be empty or null.");
        }

        if (itemSize <= 0)
        {
            throw new ArgumentException("Item size must be greater than 0.");
        }

        if (queueSize <= 0)
        {
            throw new ArgumentException("Queue size must be greater than 0.");
        }
        
        _metaMemory = new FixedMbMemory<FixedMbQueueMeta>($"fmbq-{name}-meta", 1048576, 1048576); // 5MB single block store. TODO Calculate size of meta based on item count.
        _itemMemory = new FixedMbMemory<T>($"fmbq-{name}", itemSize, itemSize * queueSize);
        _mutex = new Mutex(false, $"fmbq-{name}");

        var meta = GetMetadata().GetAwaiter().GetResult();

        if (meta == null)
        {
            WriteMetadata(new FixedMbQueueMeta(queueSize, itemSize)).GetAwaiter().GetResult();
        }
        
        meta = GetMetadata().GetAwaiter().GetResult();

        if (meta == null)
        {
            throw new Exception("Unable to persist metadata.");
        }
        
        if (meta.ItemSize != itemSize || meta.Size != queueSize)
        {
            throw new ArgumentException("A queue with the same name already exists with a different size and/or item size.");
        }
    }

    public async Task Enqueue(T item)
    {
        _mutex.WaitOne();

        try
        {
            var meta = await RequireMetadata();
            var nextPosition = Enumerable.Range(0, _itemMemory.Size).Where(x => meta.Items.All(y => y.Position != x)).OrderBy(x => x).FirstOrDefault();
            var nextMemoryIndex = Enumerable.Range(0, _itemMemory.Size).Where(x => meta.Items.All(y => y.MemoryIndex != x)).OrderBy(x => x).FirstOrDefault();

            if (nextPosition == default && meta.Items.Any(x => x.Position == nextPosition))
            {
                throw new InvalidOperationException("Queue is full.");
            }
            
            if (nextMemoryIndex == default && meta.Items.Any(x => x.MemoryIndex == nextMemoryIndex))
            {
                throw new Exception($"Memory index \"{nextMemoryIndex}\" is already in use.");
            }
            
            meta.Items.Add(new FixedMbQueueItemMeta(nextPosition, nextMemoryIndex));
            await _itemMemory.WriteAsync(nextMemoryIndex, item);
            await WriteMetadata(meta);
        }
        finally
        {
            _mutex.ReleaseMutex();
        }
    }

    public bool TryDequeue(out T? item)
    {
        item = default;
        
        _mutex.WaitOne();

        try
        {
            var meta = RequireMetadata().GetAwaiter().GetResult();
            var nextItemMeta = meta.Items.MinBy(x => x.Position);

            if (nextItemMeta == default)
            {
                return false;
            }

            item = _itemMemory.ReadAsync(nextItemMeta.MemoryIndex).GetAwaiter().GetResult();

            if (item == null)
            {
                throw new Exception("Expected item but found null.");
            }
            
            _itemMemory.DeleteAsync(nextItemMeta.MemoryIndex).GetAwaiter().GetResult();
            meta.Items.Remove(nextItemMeta);

            foreach (var itemMeta in meta.Items.Where(x => x.Position > nextItemMeta.Position))
            {
                itemMeta.Position--;
            }

            WriteMetadata(meta).GetAwaiter().GetResult();

            return true;
        }
        finally
        {
            _mutex.ReleaseMutex();
        }
    }

    public async Task Clear()
    {
        _mutex.WaitOne();

        try
        {
            await _itemMemory.ClearAsync();
            var meta = await RequireMetadata();
            
            meta.Items.Clear();

            await WriteMetadata(meta);
        }
        finally
        {
            _mutex.ReleaseMutex();
        }
    }

    public void Dispose()
    {
          _mutex.Dispose();
          _metaMemory.Dispose();
          _itemMemory.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        _mutex.Dispose();
        await _metaMemory.DisposeAsync();
        await _itemMemory.DisposeAsync();
    }

    private async Task<FixedMbQueueMeta> RequireMetadata()
    {
        var metadata = await GetMetadata();

        if (metadata == null)
        {
            throw new Exception("Metadata is no longer accessible.");
        }

        return metadata;
    }
    
    private async Task<FixedMbQueueMeta?> GetMetadata()
    {
        return await _metaMemory.ReadAsync(0);
    }
    
    private async Task WriteMetadata(FixedMbQueueMeta metadata)
    {
        await _metaMemory.WriteAsync(0, metadata);
    }
}