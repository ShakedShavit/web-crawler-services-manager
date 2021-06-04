const {
    setHashValuesInRedis,
    getHashValuesFromRedis,
} = require('./utils/redis');
const {
    createQueue,
    deleteQueue
} = require('./utils/sqs');

const crawlReachedNextLevel = async (queueName, crawlHashKey) => {
    let [currentLevel, currQueueUrl, nextQueueUrl, maxDepth, currLvlLinksLen, nextLvlLinksLen] = await getHashValuesFromRedis(crawlHashKey, ['currentLevel', 'currQueueUrl', 'nextQueueUrl', 'maxDepth', 'currLvlLinksLen', 'nextLvlLinksLen']);
    currentLevel = parseInt(currentLevel);
    currLvlLinksLen = parseInt(currLvlLinksLen);
    nextLvlLinksLen = parseInt(nextLvlLinksLen);

    const newLevel = currentLevel + 1;
    let shouldCreateQueue = true;
    if (!!maxDepth && maxDepth !== "null") {
        if (newLevel > parseInt(maxDepth)) return true;
        if (newLevel === parseInt(maxDepth)) shouldCreateQueue = false;
    }

    deleteQueue(currQueueUrl);
    let newQueue = '';
    if (shouldCreateQueue) newQueue = await createQueue(queueName, newLevel + 1);
    await setHashValuesInRedis(crawlHashKey, ['currentLevel', newLevel, 'currQueueUrl', nextQueueUrl, 'nextQueueUrl', newQueue, 'lvlPageCounter', 0, 'currLvlLinksLen', nextLvlLinksLen, 'nextLvlLinksLen', 0]);

    return false;
}

module.exports = {
    crawlReachedNextLevel
};