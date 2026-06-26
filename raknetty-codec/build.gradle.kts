dependencies {
    api(project(":raknetty-core"))
    api(libs.netty.buffer)
    api(libs.netty.transport)

    testImplementation(libs.netty.buffer)
}
