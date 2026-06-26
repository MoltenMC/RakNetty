dependencies {
    api(project(":raknetty-handler"))
    api(libs.netty.transport)

    testImplementation(libs.netty.transport)
    testImplementation(libs.netty.buffer)
}
