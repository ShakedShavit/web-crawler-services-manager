const { getElementsFromListInRedis, removeElementFromListInRedis } = require("./utils/redis");
const updateCrawlTree = require("./updateCrawlTree");

const getCrawlNameFromRedis = (crawlListKey) => {
    return new Promise((resolve, reject) => {
        getElementsFromListInRedis(crawlListKey, 0, 0)
            .then(([res]) => {
                if (!res) reject(res);
                resolve(res);
            })
            .catch((err) => {
                reject(err);
            });
    });
};

const startBuildingTreeProcess = async () => {
    const crawlListKey = "crawl-name-list";
    let crawlName;
    while (true) {
        try {
            crawlName = await getCrawlNameFromRedis(crawlListKey);
        } catch (err) {
            console.log("researching...");
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
        }

        try {
            console.time("CRAWL_DURATION");
            console.log(crawlName);
            await updateCrawlTree(crawlName);
            await removeElementFromListInRedis(crawlListKey, crawlName);
            console.timeEnd("CRAWL_DURATION");
            await new Promise((resolve) => setTimeout(resolve, 2000));
        } catch (err) {
            console.timeEnd("CRAWL_DURATION");
            console.log(err.message, err);
            continue;
        }
    }
};

startBuildingTreeProcess();
