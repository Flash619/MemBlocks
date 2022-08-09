using System.Text.Json.Serialization;

namespace MemBlocks;

public class FixedMbQueueMeta
{
    public List<FixedMbQueueItemMeta> Items { get; } = new();
    public int Size { get; }
    public int ItemSize { get; }

    [JsonConstructor]
    public FixedMbQueueMeta(List<FixedMbQueueItemMeta> items, int size, int itemSize) : this(size, itemSize)
    {
        Items = items;
    }

    public FixedMbQueueMeta(int size, int itemSize)
    {
        Size = size;
        ItemSize = itemSize;
    }
}