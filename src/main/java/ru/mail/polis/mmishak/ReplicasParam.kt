package ru.mail.polis.mmishak

data class ReplicasParam(val ask: Int, val from: Int) {

    init {
        when {
            ask <= 0 -> throw IllegalArgumentException("'ask' is not positive")
            from <= 0 -> throw IllegalArgumentException("'from' is not positive")
            ask > from -> throw IllegalArgumentException("'ask' more than 'from'")
        }
    }

    companion object {

        fun byRequestParam(replicas: String): ReplicasParam {
            try {
                val (ask, from) = replicas.split('/').map { it.toInt() }
                return ReplicasParam(ask, from)
            } catch (e: Exception) {
                throw IllegalArgumentException("Wrong format of 'replicas' param", e)
            }
        }

        fun byNodeCount(count: Int) = ReplicasParam(count / 2 + 1, count)
    }
}