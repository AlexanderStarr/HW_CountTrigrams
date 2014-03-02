from mrjob.job import MRJob
from mrjob.step import MRStep

class Trigram(MRJob):

    def mapper(self, _, line):
        '''
        This mapper expects a text file.  It will count each trigram.
        '''
        currWords = line.split()
        try:
            prevLine = self.PrevLine
        except:
            prevLine = ''
            self.PrevLine = prevLine

        prevWords = prevLine.split()
        #if len(prevWords) < 3:
        for i in range(len(currWords) - 2):
            trigram = " ".join(currWords[i:i+3])
            yield (trigram, 1)

    def reducer(self, key, values):
        yield (key, len(list(values)))

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       reducer=self.reducer)]

if __name__ == '__main__':
    Trigram.run()