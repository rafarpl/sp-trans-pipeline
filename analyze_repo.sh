#!/bin/bash

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║     🔍 ANÁLISE COMPLETA DO REPOSITÓRIO                    ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

# 1. ESTRUTURA ATUAL
echo "📂 ESTRUTURA ATUAL DO REPOSITÓRIO:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
tree -L 3 -I '__pycache__|*.pyc|venv|.git' . 2>/dev/null || find . -maxdepth 3 -not -path '*/\.*' -not -path '*/venv/*' -not -path '*/__pycache__/*' | sort
echo ""

# 2. ARQUIVOS POR TIPO
echo "📊 DISTRIBUIÇÃO DE ARQUIVOS:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Python (.py):        $(find . -name "*.py" -not -path "./venv/*" | wc -l) arquivos"
echo "SQL (.sql):          $(find . -name "*.sql" | wc -l) arquivos"
echo "Markdown (.md):      $(find . -name "*.md" | wc -l) arquivos"
echo "YAML (.yml/.yaml):   $(find . -name "*.yml" -o -name "*.yaml" | wc -l) arquivos"
echo "JSON (.json):        $(find . -name "*.json" | wc -l) arquivos"
echo "Texto (.txt):        $(find . -name "*.txt" | wc -l) arquivos"
echo "Outros:              $(find . -type f -not -path "./venv/*" -not -path "./.git/*" | wc -l) arquivos"
echo ""

# 3. TAMANHOS
echo "💾 TAMANHO POR DIRETÓRIO:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
du -sh */ 2>/dev/null | grep -v venv | sort -hr
echo ""
echo "Total do projeto (sem venv): $(du -sh --exclude=venv --exclude=.git . | cut -f1)"
echo ""

# 4. ARQUIVOS GRANDES
echo "📦 ARQUIVOS MAIORES QUE 1MB:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
find . -type f -size +1M -not -path "./venv/*" -not -path "./.git/*" -exec ls -lh {} \; 2>/dev/null | awk '{print $5, $9}' | sort -hr
echo ""

# 5. CACHE E TEMPORÁRIOS
echo "🗑️  ARQUIVOS DE CACHE E TEMPORÁRIOS:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Python cache:"
find . -type d -name "__pycache__" 2>/dev/null | wc -l
echo "  Diretórios: $(find . -type d -name "__pycache__" 2>/dev/null | wc -l)"
echo "  Arquivos .pyc: $(find . -name "*.pyc" 2>/dev/null | wc -l)"
echo ""
echo "Outros cache:"
echo "  .pytest_cache: $(find . -type d -name ".pytest_cache" 2>/dev/null | wc -l)"
echo "  .mypy_cache: $(find . -type d -name ".mypy_cache" 2>/dev/null | wc -l)"
echo "  *.egg-info: $(find . -type d -name "*.egg-info" 2>/dev/null | wc -l)"
echo "  .DS_Store: $(find . -name ".DS_Store" 2>/dev/null | wc -l)"
echo "  *~ (backup): $(find . -name "*~" 2>/dev/null | wc -l)"
echo "  *.log: $(find . -name "*.log" 2>/dev/null | wc -l)"
echo ""

# 6. GIT STATUS
echo "📝 STATUS DO GIT:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ -d .git ]; then
    echo "Arquivos não rastreados:"
    git status --short | grep "^??" | wc -l
    echo ""
    echo "Arquivos modificados:"
    git status --short | grep "^ M" | wc -l
else
    echo "⚠️  Não é um repositório Git"
fi
echo ""

