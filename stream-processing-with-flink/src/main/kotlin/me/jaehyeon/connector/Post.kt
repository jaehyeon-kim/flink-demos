package me.jaehyeon.connector

import kotlinx.serialization.Serializable
import java.io.Serializable as JavaSerializable

/**
 * Data class representing a Post, fetched from the external API.
 *
 * - @Serializable: For Ktor to deserialize JSON into this object.
 * - JavaSerializable: For Flink to send objects between TaskManagers.
 *   An alias is used to avoid a name clash.
 */
@Serializable
data class Post(
    val userId: Int,
    val id: Int,
    val title: String,
    val body: String,
) : JavaSerializable
