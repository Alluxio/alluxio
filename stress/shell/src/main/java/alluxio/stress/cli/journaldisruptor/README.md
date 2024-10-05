#Journal Disruptor#

The Journal Disruptor is aim to help us manipulate the binary journal file (both ufs journal and embedded journal).

For now by run command './bin/alluxio runClass alluxio.stress.cli.journalTool.JournalTool' we can call the journal disruptor.

Use param -OutputDir to set the directory of generated binary journal file.

The journal disruptor will read the binary journal file from the journal directory, do something on them, and write it back following the protobuf format, which means the sequence number, EOF, journal file name, and etc. will all conform to the right way.

The journal disruptor is still far from complete. Thus, here will describe the ideal structure and the way it works first, then will describe how it works now.

The journal disruptor should have a journal reader and a journal writer. The journal reader will read journal from a entry stream, so the disruptor will process the journal as a stream, and write them back with it's journal writer. The specific disrupt behavior should be defined in the disrupt() function.

Currently the disrupter only read and process the journal entry, and then return the entry to the upper class and let the upper class write entries back.

Attention:
When writing journal entries back by the writer we should clear the sequencenumber(), because raft journal wirter can write no more than two entries once, and the writer has it own logic to deal with the sequence number.

Now the JournalReader contains a EntryStream, and the EntryStream was implemented by UfsJournalEntryStream and RaftJournalEntryStream. So the JournalReader is redundent...

Now the journal tool use test() function to work, it should use the function disrupt(), which is waiting to be complete.
