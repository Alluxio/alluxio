package tachyon.master;

/** Type of Image entry. */
enum ElementType {
  Version, Checkpoint, InodeFile, InodeFolder, RawTable, Dependency,
}