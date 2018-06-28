package ir.rkr.cacheservice.util

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.security.SecureRandom
import java.util.*


val __secureRandom = SecureRandom()
inline fun <reified T> List<T>.randomItem() =
        if (isEmpty()) Optional.empty() else Optional.of(get(__secureRandom.nextInt(size)))

inline fun <reified T> Gson.fromJson(json: String) =
        this.fromJson<T>(json, object: TypeToken<T>() {}.type)
