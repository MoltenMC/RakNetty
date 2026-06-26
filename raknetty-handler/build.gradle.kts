dependencies {
    api(project(":raknetty-codec"))
    api(libs.netty.transport)

    testImplementation(libs.netty.buffer)
}
