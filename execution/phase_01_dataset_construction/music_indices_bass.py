import re
import numpy as np

def calculate_bass_indices(data):
    """
    Calculates synthetic musical indices (0 to 1) from visual Bass tab matrix.
    """
    content = data.get("content", "")
    
    # 1. Extract [tab] blocks
    tab_blocks = re.findall(r'\[tab\](.*?)\[/tab\]', content, re.DOTALL)
    
    res = {
        "complexity": 0.0,
        "repetition": 0.0,
        "melodicness": 0.0,
        "energy": 0.0,
        "finger_movement": 0.0,
        "disruption": 0.0,
        "root_stability": 0.0,
        "intra_root_variation": 0.0,
        "harmonic_palette": 0.0,
        "loop_strength": 0.0,
        "structure_variation": 0.0,
        "playability": 0.0,
        "harmonic_softness": 0.0
    }
    
    all_notes = [] # list of dicts: {"string": idx, "fret": int}
    total_characters = 0
    string_jumps = 0
    fret_movement = 0
    
    string_weight = {"g": 4, "d": 3, "a": 2, "e": 1} # for string jump severity
    
    for block in tab_blocks:
        lines = [l.strip() for l in block.split('\n') if '|' in l and len(l) > 4]
        if not lines:
             continue
             
        # Normalize to standard 4 string setup where possible or list lines
        parsed_lines = []
        string_labels = []
        for line in lines:
             match = re.match(r'^([A-Za-z]b?)\|', line)
             if match:
                  label = match.group(1).lower()
                  string_labels.append(label)
                  # Take only content after pipe |
                  parsed_lines.append(line.split('|', 1)[1])
                  
        if not parsed_lines:
             continue
             
        max_len = max(len(l) for l in parsed_lines)
        total_characters += max_len
        
        # Traverse column by column
        for col_idx in range(max_len):
             col_notes = []
             for str_idx, line in enumerate(parsed_lines):
                  if col_idx < len(line):
                       char = line[col_idx]
                       if char.isdigit():
                            col_notes.append({
                                "string": string_labels[str_idx],
                                "fret": int(char)
                            })
             
             if col_notes:
                  all_notes.extend(col_notes)

    total_notes = len(all_notes)
    if total_notes == 0:
        return {k: None for k in res.keys()}
        
    frets = [n["fret"] for n in all_notes]
    unique_frets = list(set(frets))
    
    # 1. Harmonic Palette (Richness of fret position usage)
    res["harmonic_palette"] = min(len(unique_frets) / 8.0, 1.0)
    
    # 2. Complexity
    # spread + note variety
    fret_span = (max(frets) - min(frets)) if frets else 0
    res["complexity"] = 0.4 * (len(unique_frets) / total_notes) + 0.6 * (fret_span / 24.0)
    
    # 3. Repetition
    res["repetition"] = 1.0 - (len(unique_frets) / total_notes) if total_notes > 1 else 1.0
    
    # 4. Energy (Note density over string length)
    density = total_notes / total_characters if total_characters > 0 else 0.0
    res["energy"] = min(density * 10, 1.0) # Assume 1 note every 10 chars is normal
    
    # 5. Finger Movement
    for i in range(1, total_notes):
         prev = all_notes[i-1]
         curr = all_notes[i]
         fret_movement += abs(curr["fret"] - prev["fret"])
         if curr["string"] != prev["string"]:
              string_jumps += 1
              
    res["finger_movement"] = (fret_movement + string_jumps) / total_notes if total_notes > 0 else 0.0
    res["finger_movement"] = min(res["finger_movement"] / 5.0, 1.0) # Scaling
    
    # 6. Playability
    res["playability"] = max(1.0 - res["complexity"], 0.0)
    
    # 7. Structure Variation
    # Standard fallback from bracket structure from previous algorithm
    structure_matches = re.findall(r'\[(?!ch|/ch|tab|/tab)([^\]]+)\]', content)
    unique_sections = len(set(structure_matches))
    res["structure_variation"] = min(unique_sections / 6.0, 1.0)
    
    # 8. Disruption
    res["disruption"] = (string_jumps / total_notes) * (fret_movement / total_notes) if total_notes > 0 else 0.0
    res["disruption"] = min(res["disruption"], 1.0)

    # 9. fill other fields with proxy or fallback
    res["melodicness"] = 1.0 - res["disruption"]
    res["root_stability"] = 1.0 - (string_jumps / total_notes) if total_notes > 0 else 1.0
    res["harmonic_softness"] = 1.0 - (max(frets) / 24.0) if frets else 1.0

    return res
