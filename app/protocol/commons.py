# No tagged fields for this challenge | Ref.: https://app.codecrafters.io/courses/kafka/stages/wa6
# => Just COMPACT_ARRAY with no elements => 0 on 1 byte (derived from empirical observations)
TAG_BUFFER = int(0).to_bytes(1, byteorder="big")


def get_size_representation(arr):
    # stored as 0 for 0 or N + 1 else
    return len(arr) + (len(arr) > 0)
