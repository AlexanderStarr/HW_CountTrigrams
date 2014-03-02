from mrjob.job import MRJob
from mrjob.step import MRStep

NGRAM = 3

class Trigram(MRJob):

    def mapper(self, _, line):
        '''
        This mapper expects a text file.  It will count each trigram.
        '''
        # Get the list of words from this line.
        currWords = line.split()

        # Find or create the list of the previous line's words.
        try:
            prevWords = self.PrevWords
        except:
            prevWords = []
            self.PrevWords = prevWords

        # We want to include the last 2 words from the previous line
        # in our trigrams of this line, so attach them to our word list.
        # Note: this works even if len(prevWords) < 2.
        currWords = prevWords[-(NGRAM-1):] + currWords
        for i in range(len(currWords) - (NGRAM-1)):
            new_ngram = " ".join(currWords[i:i+NGRAM])
            yield (new_ngram, 1)

        # Now save the currWords as the previous line's words.
        self.PrevWords = currWords

    def reducer(self, key, values):
        yield (key, len(list(values)))

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       reducer=self.reducer)]

if __name__ == '__main__':
    Trigram.run()