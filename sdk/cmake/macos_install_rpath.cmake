# Injected through CMAKE_PROJECT_INCLUDE; upstream repositories stay unmodified.
include_guard(GLOBAL)

if(NOT APPLE)
    return()
endif()

function(_dds_workspace_set_macos_install_rpath)
    foreach(target IN ITEMS fastcdr fastdds fast-discovery-server)
        if(TARGET "${target}")
            get_target_property(is_imported "${target}" IMPORTED)
            if(NOT is_imported)
                # Use the final RPATH at link time, avoiding repeated in-place
                # install_name_tool edits during incremental installation.
                set_target_properties("${target}" PROPERTIES
                    BUILD_WITH_INSTALL_RPATH ON
                    INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib"
                )
            endif()
        endif()
    endforeach()
endfunction()

cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}"
    CALL _dds_workspace_set_macos_install_rpath)

