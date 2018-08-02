package ir.rkr.cacheservice.util

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.security.SecureRandom
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue


val __secureRandom = SecureRandom()
inline fun <reified T> List<T>.randomItem() =
        if (isEmpty()) Optional.empty() else Optional.of(get(__secureRandom.nextInt(size)))

inline fun <reified T> Gson.fromJson(json: String) =
        this.fromJson<T>(json, object: TypeToken<T>() {}.type)

data class ExecResult<T>(val result: T, val millis: Long) {
    val seconds get() = millis / 1000.0
}


inline fun <reified T> measureExecutionTime(block: () -> T): ExecResult<T> {
    val start = System.currentTimeMillis()
    val t = block()
    return ExecResult(t, System.currentTimeMillis() - start)
}

fun <T> BlockingQueue<T>.takeMax(number:Int): MutableList<T>{
    if (number <= 0)
        mutableListOf<T>()

    val result = mutableListOf<T>()
    result.add(take())
    for (j in 1..(number - 1)) {
        val element = poll() ?: break
        result.add(element)
    }

    return result
}

