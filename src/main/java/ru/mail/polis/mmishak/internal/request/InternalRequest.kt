package ru.mail.polis.mmishak.internal.request

import one.nio.http.HttpClient
import java.util.concurrent.Callable

abstract class InternalRequest<T>(val id: String, val client: HttpClient) : Callable<T>