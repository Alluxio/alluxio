package tachyon.master;

/** Type of Image entry. */
enum ElementType {
  Checkpoint, InodeFile, InodeFolder, RawTable, Dependency,
}