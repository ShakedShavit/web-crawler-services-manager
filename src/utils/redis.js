const redisClient = require('../db/redis');

const doesKeyExistInRedis = async (key) => {
    try {
        return redisClient.existsAsync(key);
    } catch (err) {
        throw new Error(err.message);
    }
}

const setHashValuesInRedis = async (hashKey, data) => {
    try {
        const doesKeyExist = await doesKeyExistInRedis(hashKey);
        if (!doesKeyExist) throw new Error('key does not exist in redis');

        return redisClient.hsetAsync(hashKey, ...data);
    } catch (err) {
        throw new Error(err.message);
    }
}

const getHashValuesFromRedis = async (hashKey, fieldsArr) => {
    try {
        return redisClient.hmgetAsync(hashKey, ...fieldsArr);
    } catch (err) {
        throw new Error(err.message);
    }
}

const incHashIntValInRedis = async (hashKey, field, factor = 1) => {
    try {
        const doesKeyExist = await doesKeyExistInRedis(hashKey);
        // Return 0 or 1 (!0 equals True, !1 equals False)
        if (!doesKeyExist) throw new Error('key does not exist in redis');
        
        if (typeof factor !== 'number') {
            let prevFactor = factor;
            factor = parseInt(factor);
            if (isNaN(factor)) throw new Error(`factor's type must be number. factor (${prevFactor}) input is of type ${typeof prevFactor}`);
        }
        if (factor === 0) return 0;

        return redisClient.hincrbyAsync(hashKey, field, factor);
    } catch (err) {
        console.log(err.message, '37');
        throw new Error(err.message);
    }
}

const appendElementsToListInRedis = async (key, elementsArr) => {
    try {
        return redisClient.rpushAsync(key, ...elementsArr);
    } catch (err) {
        throw new Error(err.message);
    }
}

const appendElementsToStartOfListInRedis = async (key, elementsArr) => {
    try {
        return redisClient.lpushAsync(key, ...elementsArr);
    } catch (err) {
        throw new Error(err.message);
    }
}

const getElementsFromListInRedis = async (key, start = 0, end = -1) => {
    try {
        return redisClient.lrangeAsync(key, start, end);
    } catch (err) {
        throw new Error(err.message);
    }
}

const popFirstElementOfListInRedis = async (key) => {
    try {
        return redisClient.lpopAsync(key);
    } catch (err) {
        throw new Error(err.message);
    }
}


const removeElementFromListInRedis = async (key, element, count = 0) => {
    try {
        return redisClient.lremAsync(key, count, element);
    } catch (err) {
        throw new Error(err.message);
    }
}

module.exports = {
    setHashValuesInRedis,
    getHashValuesFromRedis,
    incHashIntValInRedis,
    appendElementsToListInRedis,
    appendElementsToStartOfListInRedis,
    getElementsFromListInRedis,
    popFirstElementOfListInRedis,
    removeElementFromListInRedis
}