import re
from pathlib import Path

def extract_lineage():
    dag_dir = Path("dags")
    lineage = []

    for dag_file in dag_dir.glob("*.py"):
        content = dag_file.read_text()
        datasets = re.findall(r"(raw|silver|gold)_[a-z_]+", content)
        if datasets:
            lineage.append({
                "dag": dag_file.name,
                "datasets": list(set(datasets))
            })

    md_output = Path("docs/07_metadata_catalog.md")
    with md_output.open("a") as f:
        f.write("\n\n## 🔄 Data Lineage Map\n")
        for item in lineage:
            f.write(f"\n- **{item['dag']}** → {', '.join(item['datasets'])}")
    print("✅ Lineage metadata appended to docs/07_metadata_catalog.md")

if __name__ == "__main__":
    extract_lineage()
