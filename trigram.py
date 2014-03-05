# Written by Alexander Starr

from mrjob.job import MRJob
from mrjob.step import MRStep

NGRAM = 3

class Trigram(MRJob):

    def mapper(self, _, line):
        '''
        This mapper expects a text file.  It will count each trigram in
        the file, including punctuation and ignoring whitespace (including
        blank lines).
        '''
        # Get the list of words from this line.
        currWords = line.split()

        # Find or create the list of the previous line's words.
        try:
            prevWords = self.PrevWords
        except:
            prevWords = []
            self.PrevWords = prevWords

        # We want to include the last NGRAM-1 words from the previous line
        # in our n-grams of this line, so attach them to our word list.
        # Note: this works even if len(prevWords) < NGRAM-1.
        currWords = prevWords[-(NGRAM-1):] + currWords

        # Now iterate through the word list, and yield a new n-gram for each
        # 'window' of length NGRAM in the list.
        for i in range(len(currWords) - (NGRAM-1)):
            new_ngram = " ".join(currWords[i:i+NGRAM])
            yield (new_ngram, 1)

        # Now save the currWords as the previous line's words.
        # Note that it is possible to accumulate words across lines
        # if a line is < NGRAM.  This is what we want, as it will calculate
        # n-grams across small lines, paragraphs, etc.
        self.PrevWords = currWords

    def reducer(self, key, values):
        yield (key, len(list(values)))

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       reducer=self.reducer)]

if __name__ == '__main__':
    Trigram.run()
