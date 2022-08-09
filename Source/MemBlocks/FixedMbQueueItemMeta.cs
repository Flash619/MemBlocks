namespace MemBlocks;

public class FixedMbQueueItemMeta
{
    public int Position { get; set; }
    public int MemoryIndex { get; }

    public FixedMbQueueItemMeta(int position, int memoryIndex)
    {
        Position = position;
        MemoryIndex = memoryIndex;
    }
}