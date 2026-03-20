import os

def generate_tree(dir_path, prefix=""):
    tree_str = ""
    exclude = {'.git', '.tmp', '__pycache__', '.DS_Store', '.gemini'}
    
    try:
        items = sorted(os.listdir(dir_path))
    except PermissionError:
        return ""
        
    items = [item for item in items if item not in exclude]
    
    for i, item in enumerate(items):
        path = os.path.join(dir_path, item)
        is_last = (i == len(items) - 1)
        connector = "└── " if is_last else "├── "
        
        tree_str += f"{prefix}{connector}{item}\n"
        
        if os.path.isdir(path):
            extension = "    " if is_last else "│   "
            tree_str += generate_tree(path, prefix + extension)
            
    return tree_str

def update_readme():
    tree = generate_tree(".")
    readme_path = "README.md"
    
    if not os.path.exists(readme_path):
        # Create a default README if it doesn't exist
        content = """# Sound of Culture

## Obiettivi del Progetto
Costruire un dataset di canzoni degli ultimi 40 anni a partire dalle tablature (basso, chitarra, batteria, piano).

## Struttura Cartelle
<!-- STRUCTURE_START -->
<!-- STRUCTURE_END -->

## Esecuzione
Seguire le direttive in `directives/` per ogni step.
"""
        with open(readme_path, 'w') as f:
            f.write(content)
            
    with open(readme_path, 'r') as f:
        content = f.read()
        
    start_tag = "<!-- STRUCTURE_START -->"
    end_tag = "<!-- STRUCTURE_END -->"
    
    if start_tag in content and end_tag in content:
        before = content.split(start_tag)[0]
        after = content.split(end_tag)[1]
        new_content = f"{before}{start_tag}\n```text\n{tree}```\n{end_tag}{after}"
    else:
        # If tags are missing, append
        new_content = content + f"\n## Struttura Cartelle\n{start_tag}\n```text\n{tree}```\n{end_tag}\n"
        
    with open(readme_path, 'w') as f:
        f.write(new_content)

if __name__ == "__main__":
    update_readme()
