#!/usr/bin/env bash
set -euo pipefail
OLD="${1:?old}"; NEW="${2:?new}"

tmp=".sync/report-tmp"; rm -rf "$tmp"; mkdir -p "$tmp"
olist="$tmp/old.lst"; nlist="$tmp/new.lst"; commn="$tmp/common.lst"; mod="$tmp/modified.lst"

( [ -d "$OLD" ] && find "$OLD" -type f -name "*.java" | sed "s#^$OLD/##" || true ) | sort > "$olist"
find "$NEW" -type f -name "*.java" | sed "s#^$NEW/##" | sort > "$nlist"
comm -12 "$olist" "$nlist" > "$commn" || true
: > "$mod"; while read -r rel; do diff -q "$OLD/$rel" "$NEW/$rel" >/dev/null || echo "$rel" >> "$mod"; done < "$commn"

mapfile -t OWNED < <(grep -vE '^\s*#|^\s*$' .github/sync/auto_merge_restricted.txt || true)
is_owned() {
  local p="$1"; for pat in "${OWNED[@]:-}"; do [[ "$p" == $pat ]] && return 0; done; return 1;
}

adds="$tmp/added.lst"; comm -13 "$olist" "$nlist" > "$adds" || true
: > "$tmp/manual.lst"
while read -r rel; do is_owned "src/${rel#src/}" && echo "$rel" >> "$tmp/manual.lst"; done < "$mod"
while read -r rel; do is_owned "src/${rel#src/}" && echo "$rel" >> "$tmp/manual.lst"; done < "$adds"
sort -u "$tmp/manual.lst" -o "$tmp/manual.lst"

JP_VER="3.25.9"; JP_JAR="$tmp/jp.jar"
curl -sSL -o "$JP_JAR" "https://repo1.maven.org/maven2/com/github/javaparser/javaparser-core/${JP_VER}/javaparser-core-${JP_VER}.jar"
cat > "$tmp/MethodDiff.java" <<'JAVASRC'
import com.github.javaparser.*; import com.github.javaparser.ast.*; import com.github.javaparser.ast.body.*;
import java.nio.file.*; import java.util.*; import java.util.stream.*;
public class MethodDiff {
  static Set<String> sigs(Path f) throws Exception {
    Set<String> out=new TreeSet<>(); CompilationUnit cu=StaticJavaParser.parse(f);
    for (ClassOrInterfaceDeclaration c: cu.findAll(ClassOrInterfaceDeclaration.class)) {
      String cls=c.getFullyQualifiedName().orElse(c.getNameAsString());
      Stream<CallableDeclaration<?>> s=Stream.concat(c.getMethods().stream().map(m->(CallableDeclaration<?>)m), c.getConstructors().stream().map(k->(CallableDeclaration<?>)k));
      s.filter(d->d.isPublic()||d.isProtected()).forEach(d->out.add(cls+"#"+d.getSignature().asString()));
    } return out;
  }
  public static void main(String[] a)throws Exception{
    Path oldRoot=Paths.get(a[0]), newRoot=Paths.get(a[1]); List<String> files=Files.readAllLines(Paths.get(a[2]));
    StringBuilder b=new StringBuilder();
    for(String rel:files){ Path o=oldRoot.resolve(rel), n=newRoot.resolve(rel);
      if(!Files.exists(n)) continue;
      Set<String> os=Files.exists(o)?sigs(o):Set.of(), ns=sigs(n);
      Set<String> add=new TreeSet<>(ns); add.removeAll(os);
      Set<String> rem=new TreeSet<>(os); rem.removeAll(ns);
      b.append("### ").append(rel).append("\n");
      if(!add.isEmpty()){ b.append("- Added methods:\n"); add.forEach(s->b.append("  - `").append(s).append("`\n")); }
      if(!rem.isEmpty()){ b.append("- Removed methods:\n"); rem.forEach(s->b.append("  - `").append(s).append("`\n")); }
      if(add.isEmpty() && rem.isEmpty()) b.append("- Signature changes: none\n");
      b.append("\n");
    }
    System.out.print(b.toString());
  }
}
JAVASRC
javac -cp "$JP_JAR" -d "$tmp" "$tmp/MethodDiff.java"

echo "# MSQ Sync Report"
echo
echo "## [IMPORTANT] Remember to update pom.xml and README.md manually"
echo
echo "## Manual port required (owned & changed)"
if [ -s "$tmp/manual.lst" ]; then
  cat "$tmp/manual.lst" | sed 's/^/- /'
  echo; echo "### Method-level diffs"
  java -cp "$tmp:$JP_JAR" MethodDiff "$OLD" "$NEW" "$tmp/manual.lst"
else
  echo "- none"
fi

echo "## Auto-applied (changed but not owned)"
auto="$tmp/auto.lst"
comm -23 "$mod" "$tmp/manual.lst" > "$auto" || true
if [ -s "$auto" ]; then sed 's/^/- /' "$auto"; else echo "- none"; fi
