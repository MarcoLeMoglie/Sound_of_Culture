import os

def generate_tree(dir_path, prefix="", depth=0, max_depth=1):
    if depth > max_depth:
        return ""
    
    tree_str = ""
    exclude = {'.git', '.tmp', '__pycache__', '.DS_Store', '.gemini', 'README.md', '.venv'}
    
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
            tree_str += generate_tree(path, prefix + extension, depth + 1, max_depth)
            
    return tree_str

def update_gemini():
    tree = generate_tree(".")
    target_path = "Gemini.md"
    
    if not os.path.exists(target_path):
        print(f"Error: {target_path} not found.")
        return
            
    with open(target_path, 'r') as f:
        content = f.read()
        
    start_tag = "<!-- STRUCTURE_START -->"
    end_tag = "<!-- STRUCTURE_END -->"
    
    if start_tag in content and end_tag in content:
        before = content.split(start_tag)[0]
        after = content.split(end_tag)[1]
        new_content = f"{before}{start_tag}\n```text\n{tree}```\n{end_tag}{after}"
    else:
        # If tags are missing, append to the structure section if found
        if "## Organizzazione File" in content:
            parts = content.split("## Organizzazione File")
            new_content = parts[0] + "## Organizzazione File\n\n" + f"{start_tag}\n```text\n{tree}```\n{end_tag}\n" + parts[1]
        else:
            new_content = content + f"\n## Struttura Cartelle\n{start_tag}\n```text\n{tree}```\n{end_tag}\n"
        
    with open(target_path, 'w') as f:
        f.write(new_content)

if __name__ == "__main__":
    update_gemini()
