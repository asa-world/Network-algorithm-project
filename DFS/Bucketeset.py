class BucketSet(object):
    header_loc = 0
    things_number = 0
    def __init__(self,buc_num):
        self.bucket_num = buc_num
        self.buckets = [None]*buc_num
        self.init_buckets(buc_num)
        self.header = self.buckets[0]

    def init_buckets(self,bucket_num):
        for i in range(bucket_num):
            self.buckets[i] = BucketSet.Bucket(i)

    def move_header(self):
        self.header_loc = ((self.header_loc + 1 )% self.bucket_num)
        self.header = self.buckets[self.header_loc]

    def Length_cal(self,length):
        return (length % self.bucket_num)

    def add_thing(self,thing):
        length = thing[0]
        id = self.Length_cal(length)
        self.buckets[id].add_thing(thing)
        self.things_number += 1

    def SetEmpty(self):
        return self.things_number == 0

    def pop_min(self):
        while self.header.is_empty():
            self.move_header()
        min_lists = self.header.pop_out()
        self.things_number = self.things_number - len(min_lists)
        return min_lists.copy()


    class Bucket(object):
        things_amount = 0
        def __init__(self,bucketId):
            self.bucket = list()
            self.id = bucketId

        def add_thing(self,thing):
            self.bucket.append(thing)
            self.things_amount += 1

        def pop_out(self):
            things = self.bucket.copy()
            self.bucket.clear()
            self.things_amount = 0
            return things

        def is_empty(self):
            return self.things_amount == 0
