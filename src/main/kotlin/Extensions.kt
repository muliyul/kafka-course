import java.util.*

fun propertiesOf(vararg values: Pair<String, Any>) = Properties().apply { putAll(values) }
