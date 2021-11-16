package elasticache.test

import org.redisson.api.RedissonClient
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.random.Random

fun main(args: Array<String>) {
    val uri = System.getProperty("uri")
    println("uri = ${uri}")
    val uris = uri.split(',')
    val password = System.getProperty("password")
    println("password = ${password}")

    val client = RedissonCommandExecutorFactory.createCluster(uris, password)

    val slots = System.getProperty("slots").split(',')
    slots.asSequence()
            .map { thread { runReading(it.toInt(), client) } }
            .toList()
            .forEach { it.join() }
}

private fun runReading(
        hashSlot: Int,
        client: RedissonClient
) {
    var maxTime = 0L
    var avgTime = 0L
    val key = findKeyForSlot(hashSlot)

    val fkey = "FAILOVER_KEY_TEST::{$key}::${System.currentTimeMillis()}"
    println("Thread: ${Thread.currentThread().id}, hashSlot: $hashSlot, key: ${fkey}")
    val randomValue = Random.nextLong()

    val bucket = client.getBucket<Long>(fkey)
    bucket.set(randomValue)

    val sleep: Long = 500
    var readCount = 0L
    var startTrouble = 0L
    var endTrouble = 0L
    while (true) {
        val startTime = System.currentTimeMillis()

        try {
            bucket.get()
            readCount++
            if (startTrouble > 0L) {
                endTrouble = startTime
            }

            Thread.sleep(sleep)
        } catch (e: Exception) {
            if (startTrouble == 0L) {
                startTrouble = startTime
            }
            println("${Instant.now()} threadId: ${Thread.currentThread().id}, key: ${fkey}, exception during failover; " +
                    "maxTime = $maxTime;\n $e \n-----")
        } finally {
            // know that it with $sleep
            val elapsedTime = System.currentTimeMillis() - startTime
            if (elapsedTime > maxTime) {
                println("${Instant.now()} ${Thread.currentThread().id}, key: ${fkey}, new request max time: ${maxTime}")
                maxTime = elapsedTime
            }
            avgTime += elapsedTime
            if (startTrouble in 1 until endTrouble) {
                println("${Instant.now()} threadId: ${Thread.currentThread().id}, key: ${fkey}, " +
                        "without response (seconds): ${TimeUnit.MILLISECONDS.toSeconds(endTrouble - startTrouble)}")
                startTrouble = 0L
                endTrouble = 0L
            }
        }
        val printPerOperation = System.getProperty("outperoperation")?.toInt()
        if (printPerOperation != null && readCount % printPerOperation == 0L) {
            val avgTimeInternal = if (readCount == 0L) 0L else avgTime / readCount
            println("${Instant.now()} threadId: ${Thread.currentThread().id}, key: ${fkey}, request max time = $maxTime " +
                    "avgTime = $avgTimeInternal readCount: $readCount")
        }
    }
}

