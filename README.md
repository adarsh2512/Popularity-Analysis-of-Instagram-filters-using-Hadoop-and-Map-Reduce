# Popularity-Analysis-of-Instagram-filters-using-Hadoop-and-Map-Reduce

Explored popularity of Instagram filters from a dataset of random 5 million photos using Eclipse

The instagram dataset contained information of random 5 million photos on Instagram. The content includes:

userId – The ID of user
photoId – The ID of the photo
createdTime – Time of the photo posted by user
filter – Filter type used in the photo
likes – Number of likes
comments – Number of comments

The goal of this project is to understand what kind of filter is popular and what is not.

Specifically, the project implemented the following two steps:

Developed a MapReduce program to count each type of filters.
Designed another pair of mapper and reducer, which takes the output from Step 1 as the input, and ranks the filters by their frequencies in a decreasing order.

The final output format is the default of Hadoop, which is key value pair separated by tab. For example,

filterAAAA 9999

filterBBB 5555

filterCC 111
