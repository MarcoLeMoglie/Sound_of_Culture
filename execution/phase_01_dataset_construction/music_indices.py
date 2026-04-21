import re
import pandas as pd
from collections import Counter

def calculate_indices(data):
    """
    Calculates synthetic musical indices (0 to 1) from raw tab JSON data.
    Based on requirements in classificazione_canzoni_regole.txt
    """
    content = data.get("content", "")
    chord_matches = re.findall(r'\[ch\](.*?)\[/ch\]', content)
    
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
    
    total_chords = len(chord_matches)
    if total_chords == 0:
        # Return None for all indices to avoid distorting the mean
        return {k: None for k in res.keys()}

        
    unique_chords = list(set(chord_matches))
    
    # parse roots (e.g., Cmaj7 -> C, F#m -> F#)
    def get_root(chord):
         match = re.match(r'^([A-G]#?b?)', chord)
         return match.group(1) if match else chord
         
    roots = [get_root(c) for c in chord_matches]
    unique_roots = list(set(roots))
    
    # 1. Harmonic Palette
    # Richness of unique chords over total expected (capped at 1.0)
    res["harmonic_palette"] = len(unique_chords) / 10.0

    
    # 2. Complexity
    # Combine Unique/Total, and Extensions ratio
    extended_patterns = r'(7|9|11|13|maj|sus|dim|aug|m7|add)'
    extended_count = sum(1 for c in chord_matches if re.search(extended_patterns, c, re.IGNORECASE))
    
    unique_ratio = len(unique_chords) / total_chords if total_chords > 0 else 0.0
    ext_ratio = extended_count / total_chords if total_chords > 0 else 0.0
    res["complexity"] = 0.4 * unique_ratio + 0.6 * ext_ratio
    
    # 3. Repetition
    # Standard inverse of Unique ratio
    res["repetition"] = 1.0 - (len(unique_chords) / total_chords) if total_chords > 1 else 1.0
    
    # 4. Melodicness
    # Stable consonances Ratio
    soft_patterns = r'(maj7|maj9|add9|sus2|sus4)'
    soft_count = sum(1 for c in chord_matches if re.search(soft_patterns, c, re.IGNORECASE))
    res["melodicness"] = (soft_count / total_chords) * 2.0
    
    # 5. Energy
    # Derived from chords changing density and difficulty
    difficulty = str(data.get("difficulty", "")).lower()
    diff_val = 0.2
    if "novice" in difficulty: diff_val = 0.1
    elif "intermediate" in difficulty: diff_val = 0.5
    elif "advanced" in difficulty: diff_val = 0.9
    
    # chord switches density
    switches = sum(1 for i in range(1, len(chord_matches)) if chord_matches[i] != chord_matches[i-1])
    switch_density = switches / total_chords if total_chords > 0 else 0.0
    res["energy"] = 0.3 * diff_val + 0.7 * switch_density
    
    # 6. Root Stability
    # Consecutive same root count
    same_root_consecutive = sum(1 for i in range(1, len(roots)) if roots[i] == roots[i-1])
    res["root_stability"] = same_root_consecutive / total_chords if total_chords > 1 else 1.0
    
    # 7. Intra-Root Variation
    # Variations of the same root
    root_vars = {}
    for c in unique_chords:
        r = get_root(c)
        root_vars[r] = root_vars.get(r, 0) + 1
    max_vars = max(root_vars.values()) if root_vars else 1
    res["intra_root_variation"] = (max_vars - 1) / 3.0
    
    # 8. Loop Strength
    # Simple loop parser (check for repeating 4-chord sublists)
    if total_chords >= 4:
         count_sequences = 0
         # Check sliding window of 4 chords repeating
         for i in range(total_chords - 8):
              window = chord_matches[i:i+4]
              next_window = chord_matches[i+4:i+8]
              if window == next_window:
                   count_sequences += 1
         res["loop_strength"] = count_sequences / (total_chords / 4)

    else:
         res["loop_strength"] = 0.0
         
    # 9. Structure Variation
    # Count of parsed structures from string (already done in data list inside create_dataset)
    # Passed implicitly, but can be extracted simply here from brackets
    structure_matches = re.findall(r'\[(?!ch|/ch|tab|/tab)([^\]]+)\]', content)
    unique_sections = len(set(structure_matches))
    res["structure_variation"] = unique_sections / 6.0
    
    # 10. Finger Movement Proxy
    # Improved: Calculate absolute fret distance between consecutive chords
    # using 'applicature' data if available.
    applicature = data.get("applicature", [])
    chord_fret_map = {}
    if isinstance(applicature, list):
        for app in applicature:
            chord_name = app.get("chord")
            variations = app.get("variations", [])
            if chord_name and variations:
                # Use the first variation (usually standard)
                first_var = variations[0]
                frets = first_var.get("frets", [])
                if frets:
                    # Average fret position of active strings
                    active_frets = [f for f in frets if f >= 0]
                    if active_frets:
                        chord_fret_map[chord_name] = sum(active_frets) / len(active_frets)

    if total_chords > 1:
        fret_jumps = []
        for i in range(1, total_chords):
            c1, c2 = chord_matches[i-1], chord_matches[i]
            p1 = chord_fret_map.get(c1)
            p2 = chord_fret_map.get(c2)
            if p1 is not None and p2 is not None:
                fret_jumps.append(abs(p1 - p2))
            else:
                # Fallback to label length difference if applicature is missing
                fret_jumps.append(abs(len(c1) - len(c2)) / 5.0)
        res["finger_movement"] = sum(fret_jumps) / len(fret_jumps) if fret_jumps else 0.0
    else:
        res["finger_movement"] = 0.0

    # 11. Playability
    res["playability"] = 1.0 - res["complexity"]
    
    # 12. Harmonic Softness
    # Inverse of energy / high extended ratio
    res["harmonic_softness"] = ext_ratio
    
    # 13. Disruption
    # Diversity proxy
    res["disruption"] = switch_density * (1.0 - res["root_stability"])

    return res
