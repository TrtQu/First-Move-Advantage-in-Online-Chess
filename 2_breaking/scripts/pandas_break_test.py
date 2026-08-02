import csv
import time

PGN_PATH = "lichess_db_standard_rated_2026-06.pgn"
CSV_PATH = "lichess_db_standard_rated_2026-06.csv"

def blazing_fast_pgn_to_csv():
    start_time = time.time()
    print("Initiating fast stream parsing...")

    fields = ['Event', 'Site', 'Date', 'White', 'Black', 'Result', 'Moves']
    
    with open(PGN_PATH, 'r', encoding='utf-8', errors='ignore') as pgn_in, \
         open(CSV_PATH, 'w', newline='', encoding='utf-8') as csv_out:
        
        writer = csv.DictWriter(csv_out, fieldnames=fields)
        writer.writeheader()
        
        current_game = {f: '' for f in fields}
        move_lines = []
        game_count = 0
        
        for line in pgn_in:
            # Strip trailing line breaks cleanly
            line = line.rstrip()
            if not line:
                continue
            
            # Identify metadata block tags using instant string prefixes
            if line.startswith('['):
                if move_lines:
                    # Save the previously collected game data
                    current_game['Moves'] = " ".join(move_lines)
                    writer.writerow(current_game)
                    game_count += 1
                    
                    if game_count % 500000 == 0:
                        print(f"Parsed {game_count:,} games... ({int(time.time() - start_time)}s elapsed)")
                    
                    # Reset variables for the next game
                    current_game = {f: '' for f in fields}
                    move_lines = []
                
                # Direct string extraction (significantly faster than regex tracking)
                if line.startswith('[Event "'): current_game['Event'] = line[8:-2]
                elif line.startswith('[Site "'): current_game['Site'] = line[7:-2]
                elif line.startswith('[Date "'): current_game['Date'] = line[7:-2]
                elif line.startswith('[White "'): current_game['White'] = line[8:-2]
                elif line.startswith('[Black "'): current_game['Black'] = line[8:-2]
                elif line.startswith('[Result "'): current_game['Result'] = line[9:-2]
            else:
                # Accumulate the moves line text
                move_lines.append(line)
                
        # Handle the very last game row left in the buffer line sequence
        if move_lines or any(current_game.values()):
            current_game['Moves'] = " ".join(move_lines)
            writer.writerow(current_game)
            game_count += 1

    print(f"\nCompleted! Parsed {game_count:,} chess games in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    blazing_fast_pgn_to_csv()
