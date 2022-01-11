# tweetHadoop
This project reads from a dataset of "tweets" and uses Hadoop to obtain information about the dataset. It takes four commandline arguments, the first of which is a parameter. This argument can either be "user" "tweet" "hashtag" or #hashtag_pair," and this specifies which parameter of the tweets the program will focus on. The second argument is a cutoff -- an integer representing a minimum score for (groups of) tweets. The third argument is the filepath to the input file (the dataset), and the fourth is a filepath to the directory the program will create where the output will be stored. If the fourth argument is the filepath of a directory that already exists, it must be changed or the directory of that name must be deleted before the file can run. Additionally, there is a variable in the driver code (Main) called `TEMP_DIR` which must also not share its name with any filepath on the local machine when the code is run.
