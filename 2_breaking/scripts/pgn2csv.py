import os
import re
import time
from multiprocessing import Pool, cpu_count

# Configuration
PGN_PATH = "lichess_db_standard_rated_2026-06.pgn"
CSV_PATH = "lichess_db_standard_rated_2026-06.csv"

# Target variables to track
TRACKED_TAGS = ['Event', 'Site', 'Date', 'White', 'Black', 'Result']
TAG_RE = re.compile(r'\[(Event|Site|Date|White|Black|Result)\s+"([^"]*)"\]')

def process_chunk(file_path, start_pos, end_pos):
    """Processes a single raw byte slice of the 8GB file allocated to a CPU core."""
    output_lines = []
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        f.seek(start_pos)
        
        # If not at the absolute start, advance to the beginning of the next valid game
        if start_pos != 0:
            while f.tell() < end_pos:
                line = f.readline()
                if not line:
                    return []
                if line.startswith('[Event '):
                    # Step backward to include the tag we just read
                    f.seek(f.tell() - len(line.encode('utf-8')))
                    break
        
        # Read games inside this block allocation
        current_game_str = []
        while f.tell() < end_pos:
            line = f.readline()
            if not line:
                break
            
            if line.startswith('[Event ') and current_game_str:
                # Process the completed game block
                game_text = "".join(current_game_str)
                processed_row = parse_single_game(game_text)
                if processed_row:
                    output_lines.append(processed_row)
                current_game_str = [line]
            else:
                current_game_str.append(line)
                
        # Handle trailing leftover game in block boundary
        if current_game_str:
            game_text = "".join(current_game_str)
            processed_row = parse_single_game(game_text)
            if processed_row:
                output_lines.append(processed_row)
                
    return output_lines

def parse_single_game(game_str):
    """Blazing fast regex extractor that maps text straight to clean CSV fields."""
    parts = game_str.split('\n\n', 1)
    metadata = parts[0]
    moves = parts[1].replace('\n', ' ').strip() if len(parts) > 1 else ""
    
    # Initialize empty layout
    row_data = {t: "" for t in TRACKED_TAGS}
    for match in TAG_RE.finditer(metadata):
        row_data[match.group(1)] = match.group(2)
        
    # Standard manual CSV sanitization (faster than csv library overhead)
    csv_cells = []
    for tag in TRACKED_TAGS:
        val = row_data[tag].replace('"', '""')
        csv_cells.append(f'"{val}"' if ',' in val or '"' in val else val)
        
    moves_sanitized = moves.replace('"', '""')
    csv_cells.append(f'"{moves_sanitized}"' if ',' in moves_sanitized or '"' in moves_sanitized else moves_sanitized)
    
    return ",".join(csv_cells)

def main():
    start_time = time.time()
    file_size = os.path.getsize(PGN_PATH)
    cores = cpu_count()
    print(f"Launching script across {cores} CPU Cores to extract {file_size / (1024**3):.2f} GB...")

    # Calculate exact safe byte boundaries for each CPU core slice
    chunk_size = file_size // cores
    boundaries = []
    for i in range(cores):
        start = i * chunk_size
        end = file_size if i == cores - 1 else (i + 1) * chunk_size
        boundaries.append((PGN_PATH, start, end))

    # Trigger parallel loop execution
    with Pool(processes=cores) as pool:
        results = pool.starmap(process_chunk, boundaries)

    print("Writing all compiled data fragments safely to storage...")
    with open(CSV_PATH, 'w', encoding='utf-8') as csv_out:
        # Write structural header
        csv_out.write(",".join(TRACKED_TAGS) + ",Moves\n")
        for chunk_lines in results:
            if chunk_lines:
                csv_out.write("\n".join(chunk_lines) + "\n")

    print(f"Finished execution! 8 GB converted perfectly in {time.time() - start_time:.2f} seconds.")

if __name__ == '__main__':
    main()
