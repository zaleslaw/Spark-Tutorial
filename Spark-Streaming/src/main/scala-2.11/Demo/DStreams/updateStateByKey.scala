package Demo.DStreams

/*
Specify function to generate new state based on previous state and new data. Example: Maintain per-user mood as state, and update it with their tweets:


def updateMood(newTweets, lastMood) => newMood
val moods = tweetsByUser.updateStateByKey(updateMood_)

 */
object updateStateByKey {

}
