const {
    setHashValuesInRedis,
    getHashValuesFromRedis,
    incHashIntValInRedis,
    appendElementsToListInRedis,
    appendElementsToStartOfListInRedis,
    getElementsFromListInRedis,
    removeElementFromListInRedis,
    popFirstElementOfListInRedis
} = require('./utils/redis');
const {
    crawlReachedNextLevel
} = require('./reachedNextLevel');

const getHashKeyForCrawl = (queueName) => {
    return `workers:${queueName}`;
}
const getPagesListKeyForCrawl = (queueName) => {
    return `pages-list:${queueName}`;
}
const getBfsUrlsListKeyForCrawl = (queueName) => {
    return `urls-bfs-list:${queueName}`;
}

// Add new page obj directly to JSON formatted tree (without parsing it)
const getUpdatedJsonTree = (treeJSON, newPageObj, parentUrl) => {
    let newPageJSON = JSON.stringify(newPageObj);
    let searchString = `"${parentUrl}","children":[`;
    let insertIndex = treeJSON.indexOf(searchString);
    if (insertIndex === -1) return newPageJSON; // If the tree is empty (first page insertion)
    insertIndex += searchString.length;
    if (treeJSON[insertIndex] === '{') newPageJSON += ',';
    return treeJSON.slice(0, insertIndex) + newPageJSON + treeJSON.slice(insertIndex);
}

const addNewPageToTree = (pageObj, treeJSON) => {
    let parentUrl = pageObj.parentUrl;
    delete pageObj.parentUrl;
    delete pageObj.linksLength;
    delete pageObj.childrenCounter;
    return getUpdatedJsonTree(treeJSON, pageObj, parentUrl);
}

const updateCrawlTree = async (queueName) => {
    const crawlHashKey = getHashKeyForCrawl(queueName);
    const redisTreeListKey = getPagesListKeyForCrawl(queueName);
    const redisBfsUrlsListKey = getBfsUrlsListKeyForCrawl(queueName);
    console.time('50 pages time');
    while (true) {
        try {
            console.time('update')
            const getPagesPromise = getElementsFromListInRedis(redisTreeListKey, 0, -1);
            const getHashValuesPromise = getHashValuesFromRedis(crawlHashKey, ['tree', 'pageCounter', 'lvlPageCounter', 'currLvlLinksLen', 'maxPages']);
            const getParentElPromise = popFirstElementOfListInRedis(redisBfsUrlsListKey);

            const results = await Promise.allSettled([getPagesPromise, getHashValuesPromise, getParentElPromise]);
            let newPages = results[0].value;
            let [treeJSON, pageCounter, lvlPageCounter, currLvlLinksLen, maxPages] = results[1].value;

            let parentEl = results[2].value;
            if (!parentEl) {
                await setHashValuesInRedis(crawlHashKey, ['isCrawlingDone', true]);
                console.timeEnd('50 pages time');
                return;
            }
            parentEl = JSON.parse(parentEl);

            pageCounter = parseInt(pageCounter);
            lvlPageCounter = parseInt(lvlPageCounter);
            currLvlLinksLen = parseInt(currLvlLinksLen);
            if (!!maxPages) maxPages = parseInt(maxPages);

            let newPagesBfsObj = [];
            let pagesBfsJSON = [];
            let promises = [];

            for (page of newPages) {
                pageObj = JSON.parse(page);
                if (pageObj.parentUrl !== parentEl.url) continue;
                newPagesBfsObj.push(pageObj);
                pagesBfsJSON.push(page);
            }

            let newPagesOriginalLen = pagesBfsJSON.length
            let newPagesLen = 0;
            for (let i = 0; i < newPagesOriginalLen; i++) {
                if (!!maxPages && maxPages < pageCounter + i) break;

                promises.push(removeElementFromListInRedis(redisTreeListKey, pagesBfsJSON[i], 1));

                if (treeJSON.includes(`"url":"${newPagesBfsObj[i].url}"`)) {
                    delete newPagesBfsObj[i].children;
                } else if (newPagesBfsObj[i].linksLength !== 0) {
                    console.log("\nadding:", pagesBfsJSON[i], "to the bfs list\n");
                    await appendElementsToListInRedis(redisBfsUrlsListKey, [pagesBfsJSON[i]]);
                }

                newPagesLen++;
                treeJSON = addNewPageToTree(newPagesBfsObj[i], treeJSON);
            }

            // If reached pages limit then stop crawling
            if (!!maxPages && maxPages <= pageCounter + newPagesLen) {
                await setHashValuesInRedis(crawlHashKey, ['tree', treeJSON, 'isCrawlingDone', true]);
                console.timeEnd('50 pages time');
                return;
            }

            // Update tree, pageCounter, and lvlPageCounter
            if (newPagesLen !== 0) {
                // await setHashValuesInRedis(crawlHashKey, ['tree', treeJSON]);
                promises.push(incHashIntValInRedis(crawlHashKey, 'pageCounter', newPagesLen));
            }
            if (newPagesOriginalLen !== 0) promises.push(incHashIntValInRedis(crawlHashKey, 'lvlPageCounter', newPagesOriginalLen));

            let isCrawlingDone = false;
            if (parentEl.linksLength > parentEl.childrenCounter + newPagesOriginalLen) {
                parentEl.childrenCounter += newPagesOriginalLen;
                promises.push(appendElementsToStartOfListInRedis(redisBfsUrlsListKey, [JSON.stringify(parentEl)]));
            } else if (lvlPageCounter + newPagesOriginalLen >= currLvlLinksLen) {
                await Promise.allSettled(promises);
                isCrawlingDone = await crawlReachedNextLevel(queueName, crawlHashKey);
            }

            promises.push(setHashValuesInRedis(crawlHashKey, ['tree', treeJSON, 'isCrawlingDone', isCrawlingDone]));
            await Promise.allSettled(promises);

            if (isCrawlingDone) {
                console.timeEnd('50 pages time');
                await setHashValuesInRedis(crawlHashKey, ['isCrawlingDone', true]);
                return;
            }

            console.timeEnd('update')
            await new Promise(resolve => setTimeout(resolve, 1000));
        } catch (err) {
            console.log(err.message, '60');
            throw new Error(err.message);
        }
    }
}

module.exports = updateCrawlTree;