using System.Collections;

namespace MemBlocks;

public class FixedMbMemoryEnumerator<T> : IEnumerator<T> where T : class
{
    public T Current { get; private set; }
    object IEnumerator.Current => Current;
    
    private int _position = -1;
    
    private readonly FixedMbMemory<T> _fixedMbMemory;
    
    internal FixedMbMemoryEnumerator(FixedMbMemory<T> fixedMbMemory)
    {
        Current = default!;
        _fixedMbMemory = fixedMbMemory;
    }
    
    public bool MoveNext()
    {
        Current = default!;
        
        _position++;

        if (_position == _fixedMbMemory.Size)
        {
            return false;
        }

        Current = _fixedMbMemory.ReadAsync(_position).GetAwaiter().GetResult()!;

        return true;
    }

    public void Reset()
    {
        _position = -1;
    }

    public void Dispose()
    {
        // Nothing to cleanup
    }
}