const express = require('express');
const cors = require('cors');
const {
    getElementsFromListInRedis,
    removeElementFromListInRedis
} = require('./utils/redis');
const updateCrawlTree = require('./updateCrawlTree');

const port = process.env.PORT || 8000;

const app = express();
app.use(cors());
app.use(express.json());

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
}

const startBuildingTreeProcess = async () => {
    const crawlListKey = 'crawl-name-list';
    let crawlName;
    while (true) {
        try {
            crawlName = await getCrawlNameFromRedis(crawlListKey);
        } catch (err) {
            console.log("researching...");
            await new Promise(resolve => setTimeout(resolve, 2000));
            continue;
        }

        try {
            console.time('a');
            console.log(crawlName);
            await updateCrawlTree(crawlName);
            await removeElementFromListInRedis(crawlListKey, crawlName);
            console.timeEnd('a');
            await new Promise(resolve => setTimeout(resolve, 2000));
        } catch (err) {
            console.timeEnd('a');
            console.log(err.message, err, '64');
            continue;
        }
    }
}

startBuildingTreeProcess();

app.listen(port, () => {
    console.log(`Server connected to port: ${port}`);
});