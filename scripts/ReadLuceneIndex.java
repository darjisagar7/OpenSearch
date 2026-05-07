import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.util.*;
import org.apache.lucene.search.*;
import java.nio.file.*;
import java.io.*;
import java.util.*;
import java.util.regex.*;

/**
 * Reads and prints documents from a Lucene index directory.
 * Opens the highest-generation segments file to handle cases where the cluster
 * is still running and the latest commit hasn't been finalized yet.
 *
 * Flags:
 *   --all       Show all segments (default: only merged)
 *   --merged    Show only merged segments (default)
 *
 * Run with: java -cp <lucene-jars> ReadLuceneIndex <index-path> [--all|--merged]
 */
public class ReadLuceneIndex {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: java ReadLuceneIndex <index-path> [--all|--merged]");
            System.exit(1);
        }

        Path indexPath = Paths.get(args[0]);
        boolean showAll = args.length > 1 && args[1].equals("--all");

        System.out.println("=== LUCENE INDEX CONTENT ===");
        System.out.println("Index path: " + indexPath);
        System.out.println();

        try (Directory dir = NIOFSDirectory.open(indexPath);
             DirectoryReader reader = openLatestReader(dir)) {

            if (reader == null) {
                System.out.println("ERROR: Could not open any segments file in " + indexPath);
                System.exit(1);
            }

            System.out.println("Total docs: " + reader.numDocs());
            System.out.println("Leaves (segments): " + reader.leaves().size());
            System.out.println();

            int shownCount = 0;
            int hiddenCount = 0;

            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leaf = ctx.reader();
                SegmentReader segReader = (SegmentReader) leaf;
                SegmentCommitInfo sci = segReader.getSegmentInfo();
                String source = sci.info.getDiagnostics().get("source");
                boolean isMerged = "merge".equals(source);

                if (!showAll && !isMerged) {
                    hiddenCount++;
                    continue;
                }

                shownCount++;
                String label = isMerged ? "[MERGED]" : "[flush]";
                System.out.println("--- " + label + " Segment: " + sci.info.name + " (docs=" + leaf.numDocs() + ", source=" + source + ") ---");

                // Print writer_generation attribute if present
                String writerGen = sci.info.getAttribute("writer_generation");
                if (writerGen != null) {
                    System.out.println("  writer_generation: " + writerGen);
                }

                // Get field names and their types
                FieldInfos fieldInfos = leaf.getFieldInfos();
                System.out.print("  Fields: ");
                for (FieldInfo fi : fieldInfos) {
                    System.out.print(fi.name + "(dv=" + fi.getDocValuesType() + ",idx=" + fi.getIndexOptions() + ") ");
                }
                System.out.println("\n");

                // Build doc->term mapping from inverted index for indexed fields
                Map<Integer, String> docToName = buildDocToTermMap(leaf, "name");

                // Read doc values
                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues("__row_id__");
                SortedNumericDocValues ageDV = leaf.getSortedNumericDocValues("age");

                System.out.printf("  %-6s %-15s %-8s %-10s%n", "doc", "name", "age", "__row_id__");
                System.out.println("  " + "-".repeat(45));

                for (int doc = 0; doc < leaf.maxDoc(); doc++) {
                    String name = docToName.getOrDefault(doc, "-");
                    String age = "-";
                    String rowId = "-";

                    if (ageDV != null && ageDV.advanceExact(doc)) {
                        age = String.valueOf(ageDV.nextValue());
                    }

                    if (rowIdDV != null && rowIdDV.advanceExact(doc)) {
                        rowId = String.valueOf(rowIdDV.nextValue());
                    }

                    System.out.printf("  %-6d %-15s %-8s %-10s%n", doc, name, age, rowId);
                }
                System.out.println();
            }

            if (shownCount == 0 && hiddenCount > 0) {
                System.out.println("No merged segments found. " + hiddenCount + " flush segments present.");
                System.out.println("Re-run with --all to show all segments.");
            } else if (!showAll && hiddenCount > 0) {
                System.out.println("(" + hiddenCount + " non-merged segments hidden. Use --all to show all.)");
            }
        }
    }

    /**
     * Opens a DirectoryReader from the highest-generation segments file.
     * Falls back through: latest commit → specific segments files by descending generation.
     * This handles the case where the cluster is still running and the NRT segments
     * haven't been committed yet (the committed segments_N may be an older empty commit).
     */
    private static DirectoryReader openLatestReader(Directory dir) throws IOException {
        // First try: standard open (uses latest commit)
        try {
            DirectoryReader reader = DirectoryReader.open(dir);
            if (reader.numDocs() > 0) {
                return reader;
            }
            // Latest commit is empty — try to find a newer segments file
            reader.close();
        } catch (IndexNotFoundException e) {
            // No commits at all
        }

        // Second try: find the highest-generation segments file and open it directly
        // This handles the case where segments_N+1 exists with data but isn't the "last commit"
        // because the IndexWriter hasn't committed yet (NRT-only state)
        String[] files = dir.listAll();
        long maxGen = -1;
        String maxSegmentsFile = null;

        Pattern segPattern = Pattern.compile("segments_(\\d+)");
        for (String file : files) {
            Matcher m = segPattern.matcher(file);
            if (m.matches()) {
                long gen = Long.parseLong(m.group(1));
                if (gen > maxGen) {
                    maxGen = gen;
                    maxSegmentsFile = file;
                }
            }
        }

        if (maxSegmentsFile == null) {
            return null;
        }

        // Open the specific commit by finding it in the list
        List<IndexCommit> commits = DirectoryReader.listCommits(dir);
        // Try from newest to oldest
        for (int i = commits.size() - 1; i >= 0; i--) {
            IndexCommit commit = commits.get(i);
            try {
                DirectoryReader reader = DirectoryReader.open(commit);
                if (reader.numDocs() > 0) {
                    return reader;
                }
                reader.close();
            } catch (Exception e) {
                // Try next commit
            }
        }

        // Last resort: open whatever the latest commit is, even if empty
        return DirectoryReader.open(dir);
    }

    /**
     * Builds a mapping from doc ID to term value by scanning the inverted index.
     * Works for single-term-per-doc fields like keyword fields.
     */
    private static Map<Integer, String> buildDocToTermMap(LeafReader leaf, String field) throws IOException {
        Map<Integer, String> docToTerm = new HashMap<>();
        Terms terms = leaf.terms(field);
        if (terms == null) return docToTerm;

        TermsEnum termsEnum = terms.iterator();
        while (termsEnum.next() != null) {
            String termText = termsEnum.term().utf8ToString();
            PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
            int doc;
            while ((doc = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
                docToTerm.put(doc, termText);
            }
        }
        return docToTerm;
    }
}
