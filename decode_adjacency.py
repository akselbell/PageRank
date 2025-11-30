import struct
import sys

def decode_adjacency(filename="adjacency.bin"):
    """Decode and print the adjacency.bin file in a readable format."""
    try:
        with open(filename, "rb") as f:
            entry_count = 0
            while True:
                # Read src_id and count (2 * 4 bytes = 8 bytes)
                header = f.read(8)
                if len(header) < 8:
                    break
                
                src_id, count = struct.unpack("<II", header)
                
                # Read destination IDs if count > 0
                dest_ids = []
                if count > 0:
                    dest_data = f.read(count * 4)
                    if len(dest_data) < count * 4:
                        print(f"Warning: Incomplete entry at src_id {src_id}")
                        break
                    dest_ids = list(struct.unpack("<" + "I" * count, dest_data))
                
                # Print the entry
                print(f"Node {src_id} -> {dest_ids}")
                entry_count += 1
            
            print(f"\nTotal entries: {entry_count}")
    
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    filename = sys.argv[1] if len(sys.argv) > 1 else "adjacency.bin"
    decode_adjacency(filename)

